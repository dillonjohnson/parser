import io
import json
import os
from datetime import datetime
from os import walk, getenv

import jinja2
import yaml
from airflow.models import DAG
from airflow.models.baseoperator import chain

from airflow_parser.providers.carte.operators.carte import CarteExecuteJobOperator, CarteCheckJobOperator, \
    CarteGenerateRuntimeParametersOperator

from airflow_parser.providers.aws.operators.aws import AthenaOperator, GlueOperator

from airflow_parser.providers.google.operators.google import BigqueryDataTransferOperator


class DAGGenerator:
    def __init__(self,
                 control_file_path: str,
                 etl_scripts_path: str,
                 dag_owners: str,
                 debugwin: bool = False):
        self.control_file_path = control_file_path
        self.etl_scripts_path = etl_scripts_path
        self.dag_owners = dag_owners
        self.debugwin = debugwin


    def generate_dags(self):
        control_file_path = getenv('CONTROL_FILE_PATH', self.control_file_path)

        args_dict = dict(
            env=getenv('ENV')
        )

        flow_specs = []
        step_specs = {}
        for (dirpath, dirnames, filenames) in walk(control_file_path):
            if dirpath.endswith('templates') or dirpath.endswith('demo1'):
                print('Skipping templates...')
                continue
            for filename in filenames:
                if filename.endswith('.yaml') or filename.endswith('.yml'):  # Ignore any file that is not a yaml
                    filepath = dirpath + '/' + filename
                    with io.open(filepath, 'r') as file:
                        file_text_template = jinja2.Template(file.read())
                        file_text = file_text_template.render()
                        yaml_object = yaml.load(file_text, Loader=yaml.FullLoader)
                    try:
                        object_type = yaml_object['type']
                    except KeyError as ex:
                        print(f'Type missing for file: {filepath}')
                        raise ex
                    if yaml_object['type'] == 'flow_specification':
                        flow_specs.append(yaml_object)
                    elif yaml_object['type'] == 'step_specification':
                        step_specs[yaml_object['name']] = yaml_object

        etl_scripts = {}
        for (dirpath, dirnames, filenames) in walk(self.etl_scripts_path):
            for filename in filenames:
                if filename.endswith('.py') or filename.endswith('.sql'):
                    filepath = dirpath + '/' + filename
                    etl_scripts[str(filepath).replace(self.etl_scripts_path, '')] = str(filepath)

        DEFAULT_ARGS = {
            'owner': self.dag_owners,
            'start_date': datetime(2020, 1, 1),  # Abitrary date in the past, won't matter since catchup=False
            'depends_on_past': False
        }

        for flow_spec in flow_specs:
            with DAG(dag_id=flow_spec['name'],
                     schedule_interval=flow_spec['schedule'],
                     catchup=False,
                     default_args=DEFAULT_ARGS,
                     max_active_runs=1) as dag:
                previous_step_ops = []
                sensor_op = None
                flow_ops = {}
                for idx, items in enumerate(flow_spec['flow']):
                    flow_ops[str(idx)] = list()
                    for s in items:
                        step = step_specs[s]
                        etl_platform = step['etl_platform']
                        if etl_platform == 'athena':
                            op = AthenaOperator(
                                task_id=str(idx) + '_' + step['name'],
                                sql=io.open('/usr/local/airflow/etl_scripts/' + step['etl_platform_filename']).read(),
                                target_database=step['target_data'][0]['database'],
                                target_table=step['target_data'][0]['table'],
                                target_s3=step['target_data'][0]['s3']
                            )
                        elif etl_platform == 'glue':
                            additional_job_config = step.get('parameters', {}).copy()
                            remove_keys = ['glue_max_capacity', 'python_version', 'version']
                            for key in remove_keys:
                                try:
                                    additional_job_config.pop(key)
                                except KeyError:
                                    continue
                            if self.debugwin:
                                this_python_local_path = etl_scripts[f'\\{step["etl_platform_filename"]}'][0]
                            else:
                                this_python_local_path = etl_scripts[f'{step["etl_platform"]}/{step["etl_platform_filename"]}'][0]

                            op = GlueOperator(
                                task_id=step['name'],
                                job_name=step['name'],
                                glue_role=os.getenv('ASSUMED_ROLE'),
                                #script_name='Demo',
                                python_file_location=f's3://{os.getenv("GLUE_BUCKET")}/{step["etl_platform_filename"]}',
                                python_local_path=this_python_local_path,
                                # python_version='3',
                                job_config=dict(source_data=json.dumps(step["source_data"]),
                                                target_data=json.dumps(step["target_data"]),
                                                **additional_job_config),
                                #**step['parameters']
                            )
                        elif etl_platform == 'bigquery_transfer':
                            op = BigqueryDataTransferOperator(
                                task_id=str(idx) + '_' + step['name'],
                                transfer_config_id=step['parameters']['transfer_config_id'],
                                project_id='124540193406',
                                wait_before_start=step['parameters'].get('wait_before_start', None),
                                truncate_target=step['parameters'].get('truncate_target', False),
                                target_table=step['target_data'][0]['table'],
                                target_database=step['target_data'][0]['database']
                            )
                        flow_ops[str(idx)].append(op)

                    previous_steps = []
                    for k, current_step_ops in flow_ops.items():
                        for current_op in current_step_ops:
                            if isinstance(current_op, list):
                                current_op = current_op[0]
                            for previous_step in previous_steps:
                                if isinstance(previous_step, list):
                                    previous_step = previous_step[2]
                                previous_step >> current_op
                        previous_steps = current_step_ops
                yield dag


if __name__ == '__main__':
    d = DAGGenerator(etl_scripts_path=r'/code/cf-airflow/etl_scripts',
                     control_file_path=r'c:\code\cf-airflow\control_files',
                     dag_owners='Jon', debugwin=True)
    dags = list(d.generate_dags())
