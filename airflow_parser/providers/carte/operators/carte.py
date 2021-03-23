import logging
from datetime import datetime, timedelta
from os import getenv
from typing import Any, Dict, List
from uuid import uuid4

from airflow.models import Variable, DagRun, DAG
import requests
import xmltodict
from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.sensors.base import BaseSensorOperator
from requests.auth import HTTPBasicAuth
from requests.models import Response
import pendulum
import json


class FailedCarteCallException(Exception):
    pass


class JobStoppedException(Exception):
    pass


class CarteGenerateRuntimeParametersOperator(BaseOperator):
    """Generates various runtime parameters."""

    def __init__(self,
                 args_list: List[str],
                 *args,
                 **kwargs):
        super(CarteGenerateRuntimeParametersOperator, self).__init__(*args, **kwargs)
        self.args_list = args_list

    def execute(self, context: Any) -> str:
        items = {}
        dag: DAG = context['dag']
        previous_run: List[DagRun] = sorted(DagRun.find(state='success',
                                                        dag_id=str(dag.dag_id)), key=lambda x: x.start_date, reverse=True)
        if len(previous_run) > 0:
            print(previous_run)
            if previous_run[0].end_date == context['dag_run'].start_date:
                previous_run_dt = datetime(2021, 1, 1)
            else:
                previous_run_dt = previous_run[0].start_date - timedelta(minutes=5)
        else:
            previous_run_dt = datetime(2021, 1, 1)
        current_run_dt: pendulum = context['dag_run'].start_date
        print('Getting vals for: ' + str(self.args_list))
        for arg in self.args_list:
            if arg == 'generated_uuid':
                items[arg] = str(uuid4())
            elif arg == 'START_TIMESTAMP':
                items[arg] = Variable.get(arg,
                                          default_var=previous_run_dt.strftime('%Y-%m-%d %H:%M:%S'))
                print('START_TIMESTAMP ' + items[arg])
            elif arg == 'END_TIMESTAMP':
                items[arg] = Variable.get(arg,
                                          default_var=current_run_dt.strftime('%Y-%m-%d %H:%M:%S'))
                print('END_TIMESTAMP ' + items[arg])
            elif arg == 'RUN_DATE':
                items[arg] = Variable.get(arg,
                                          default_var=datetime.now().strftime('%Y-%m-%d'))
                print('RUN_DATE ' + items[arg])
            elif arg == 'org_id':
                items[arg] = 1  # change in future?
            else:
                items[arg] = Variable.get(arg)  # Will error in Variable not defined

        return json.dumps(items, default=str)


class CarteExecuteJobOperator(BaseOperator):
    """Implementation to run a Carte job"""

    TRIGGER_URL = 'kettle/executeJob/'

    template_fields = ('params',)

    def __init__(self,
                 *args,
                 repository_name=None,
                 carte_user=None,
                 carte_password=None,
                 job_name=None,
                 carte_level=None,
                 host=None,
                 additional_carte_job_params=None,
                 **kwargs):
        super(CarteExecuteJobOperator, self).__init__(*args, **kwargs)
        self.repository_name = repository_name
        self.carte_user = carte_user
        self.carte_password = carte_password
        self.carte_level = carte_level
        self.job_name = job_name
        self.additional_carte_job_params = additional_carte_job_params or {}
        self.host = host

    def build_trigger_fields(self):
        """
        Builds a dictionary of values to be sent to Carte with the trigger job call

        :return:
        """
        return {k: Variable.get(k, default_var=v)
                for k, v in {'job': self.job_name,
                             'rep': self.repository_name,
                             'level': self.carte_level,
                             **self.additional_carte_job_params}.items()}

    def trigger_job(self, fields) -> str:
        """
        Triggers the Carte job and gets the trigger response

        :param fields: the additional fields to send with the request
        :return:
        """

        resp: Response = requests.get('{}/{}'.format(self.host,
                                                     self.TRIGGER_URL),
                                      auth=HTTPBasicAuth(self.carte_user, self.carte_password),
                                      params=fields)

        logging.info(f'Called URL: {resp.request.url}')

        if resp.status_code == 200:
            return resp.text
        else:
            raise FailedCarteCallException(resp.status_code, resp.text, resp.request.url)

    @staticmethod
    def parse_job_id(job_response: dict):
        """
        Gets the job_id from the response

        :param job_response: response from the Carte job trigger
        :return: UUID for the job
        """
        return job_response['webresult']['id']

    def execute(self, context: Any):
        """
        Executes from start to finish a Carte job.

        :param context: The Airflow context
        :return: The results of the Carte job
        """

        # get runtime vars
        ti: TaskInstance = context['ti']
        if not self.additional_carte_job_params:
            xcom_obj = ti.xcom_pull(task_ids='generate_runtime_params') or "{}"
            self.additional_carte_job_params = json.loads(xcom_obj)

        trigger_fields = self.build_trigger_fields()

        # Trigger job
        trigger_resp = self.trigger_job(trigger_fields)

        # Convert the response to a dict
        response_dict = xmltodict.parse(trigger_resp)

        logging.info(response_dict)

        # Get job id
        job_id = self.parse_job_id(response_dict)

        logging.info(f'Job ID: {job_id}')

        return job_id


class CarteCheckJobOperator(BaseSensorOperator):
    """Operator that monitors the completion status of a Carte job"""

    FINISHED = 'Finished'
    STOPPED = 'Stopped'
    FINISHED_WITH_ERRORS = 'Finished (with errors)'
    FINISHED_STATUSES = [FINISHED, STOPPED, FINISHED_WITH_ERRORS]

    STATUS_URL = 'kettle/jobStatus/'

    def __init__(self,
                 job_execute_task_id: str,
                 as_xml: bool = True,
                 carte_from: int = None,
                 carte_user: str = None,
                 carte_password: str = None,
                 job_name: str = None,
                 host: str = None,
                 *args,
                 **kwargs):
        super(CarteCheckJobOperator, self).__init__(*args, **kwargs)
        self.job_execute_task_id = job_execute_task_id
        self.as_xml = as_xml
        self.carte_from = carte_from
        self.job_id = None
        self.carte_user = carte_user
        self.carte_password = carte_password
        self.job_name = job_name
        self.host = host

    def get_job_id(self,
                   task_instance: TaskInstance,
                   **kwargs):
        if not self.job_id:
            self.job_id = task_instance.xcom_pull(task_ids=self.job_execute_task_id)
        return self.job_id

    def build_check_params(self,
                           job_id: str):
        return {k: v for k, v in {'job': self.job_name,
                                  'xml': 'Y' if self.as_xml else 'N',
                                  'id': job_id}.items()}

    def get_status(self, job_id: str):
        """

        :return:
        """
        resp: Response = requests.get('{}/{}'.format(self.host, self.STATUS_URL),
                                      auth=HTTPBasicAuth(self.carte_user, self.carte_password),
                                      params=self.build_check_params(job_id))

        if resp.status_code == 200:
            return resp.text
        else:
            raise FailedCarteCallException(resp.status_code, resp.text)

    def parse_status(self, status_dict: dict):
        return status_dict['jobstatus']['status_desc']

    def poke(self, context: Dict) -> bool:

        job_id = self.get_job_id(task_instance=context['ti'])

        logging.info(f'Found Job ID: {job_id}')

        status_xml = self.get_status(job_id)

        status_dict = xmltodict.parse(status_xml)

        print('Job Response: ' + str(status_xml))

        status: str = self.parse_status(status_dict)

        logging.info(f'Status found: {status}')

        if status in self.FINISHED_STATUSES:
            if status == self.FINISHED:
                return True
            else:
                raise JobStoppedException
        return False
