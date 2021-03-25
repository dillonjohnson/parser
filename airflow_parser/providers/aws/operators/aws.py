import json
import logging
from pprint import pprint
from uuid import uuid4

from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator
# from helpers.replace import replace_args, fetch_airflow_dagrun_vars
from time import sleep
# from helpers.log_helper import logs_keys
# from helpers.aws import assumed_role_session
import boto3
import os

from airflow.sensors.base import BaseSensorOperator
from botocore.exceptions import ClientError


class AthenaOperator(BaseOperator):
    """
    An operator to run queries against Athena

    :param sql: the sql to execute
    :param replace_dict: the dictionary containing symlinks
    :param parameters: parameters to place in the sql
    :param database: the database to execute the sql against
    :param output_bucket: s3 location to save Athena results
    """

    def __init__(self,
                 sql: str,
                 target_database,
                 target_table,
                 target_s3,
                 *args,
                 **kwargs):
        super(AthenaOperator, self).__init__(*args, **kwargs)

        self.sql = sql
        self.target_database = target_database
        self.target_table = target_table
        self.target_s3 = target_s3

    def execute(self, context):
        """Executes the operator"""

        # Drop target db

        # Delete files from target s3

        # Execute sql
        client = boto3.client(
            'athena'
        )

        s3 = boto3.resource(
            's3'
        )

        del_sql = f"drop table if exists {self.target_database}.{self.target_table}"
        self.log.info('Executing: %s', del_sql)
        response = client.start_query_execution(QueryString=del_sql,
                                                QueryExecutionContext={'Database': self.target_database},
                                                ResultConfiguration={
                                                    'OutputLocation': 's3://' + os.getenv("s3_output_bucket",
                                                                                          'goalcast-athena-output') + '/',
                                                    'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
                                                })

        """Checking for result"""
        checking = True
        while checking:
            query_execution_id = response['QueryExecutionId']
            query_execution = client.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_execution['QueryExecution']['Status']['State']
            if status in ('RUNNING', 'QUEUED'):
                sleep_time = 5
                sleep(sleep_time)
                print(f'Query Status {status} -- Sleeping {sleep_time} seconds')
                continue
            elif status in ('FAILED', 'CANCELLED'):
                raise Exception(f'An exception occurred while repairing table.'
                                f'\n\tStart Query Response: {response}'
                                f'\n\tQuery Execution: {query_execution}')
            elif status in ('SUCCEEDED'):
                print(f'Successful run for QueryId: {query_execution_id}')
                checking = False

        """Delete files from s3"""
        split_s3 = self.target_s3.replace('s3://', '').split('/')
        bucket_name, key = split_s3[0], '/'.join(split_s3[1:])
        bucket = s3.Bucket(bucket_name)
        for obj in bucket.objects.filter(Prefix=key):
            s3.Object(bucket.name, obj.key).delete()
        s3.Object(bucket.name, key).delete()

        """Execute sqls"""
        self.log.info('Executing: %s', self.sql)
        response = client.start_query_execution(QueryString=self.sql,
                                                QueryExecutionContext={'Database': self.target_database},
                                                ResultConfiguration={
                                                    'OutputLocation': 's3://' + os.getenv("s3_output_bucket",
                                                                                          'goalcast-athena-output') + '/',
                                                    'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
                                                })

        """Checking for result"""
        checking = True
        while checking:
            query_execution_id = response['QueryExecutionId']
            query_execution = client.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_execution['QueryExecution']['Status']['State']
            if status in ('RUNNING', 'QUEUED'):
                sleep_time = 5
                sleep(sleep_time)
                print(f'Query Status {status} -- Sleeping {sleep_time} seconds')
                continue
            elif status in ('FAILED', 'CANCELLED'):
                raise Exception(f'An exception occurred while repairing table.'
                                f'\n\tStart Query Response: {response}'
                                f'\n\tQuery Execution: {query_execution}')
            elif status in ('SUCCEEDED'):
                print(f'Successful run for QueryId: {query_execution_id}')
                checking = False


class QuicksightSpiceRefreshOperator(BaseOperator):
    """
    An operator to refresh Quicksight spices
    """

    def __init__(self,
                 quicksight_dataset_id: str,
                 aws_account_id: str,
                 *args,
                 **kwargs):
        super(QuicksightSpiceRefreshOperator, self).__init__(*args, **kwargs)
        self.quicksight_dataset_id = quicksight_dataset_id
        self.ingestion_id = None
        self.aws_account_id = aws_account_id

    def execute(self, context) -> str:
        """Executes the operator"""

        if not self.ingestion_id:
            self.ingestion_id = str(uuid4())

        # Execute sql
        client = boto3.client(
            'quicksight'
        )

        response = client.create_ingestion(DataSetId=self.quicksight_dataset_id,
                                           IngestionId=self.ingestion_id,
                                           AwsAccountId=self.aws_account_id)

        logging.info(f'Ingestion ID found: {self.ingestion_id}')

        return self.ingestion_id


class QuicksightSpiceRefreshSensor(BaseSensorOperator):
    """
    An operator to check status of spice refreshes
    """

    def __init__(self,
                 quicksight_dataset_id: str,
                 aws_account_id: str,
                 spice_trigger_task_id: str,
                 *args,
                 **kwargs):
        super(QuicksightSpiceRefreshSensor, self).__init__(*args, **kwargs)
        self.quicksight_dataset_id = quicksight_dataset_id
        self.ingestion_id = None
        self.aws_account_id = aws_account_id
        self.spice_trigger_task_id = spice_trigger_task_id

    def poke(self, context) -> bool:
        if not self.ingestion_id:
            ti: TaskInstance = context['ti']
            self.ingestion_id = ti.xcom_pull(self.spice_trigger_task_id)
        client = boto3.client('quicksight')
        response = client.describe_ingestion(DataSetId=self.quicksight_dataset_id,
                                             IngestionId=self.ingestion_id,
                                             AwsAccountId=self.aws_account_id)
        if response['Ingestion']['IngestionStatus'] in ('INITIALIZED', 'QUEUED', 'RUNNING'):
            # time.sleep(10)  # change sleep time according to your dataset size
            return False
        elif response['Ingestion']['IngestionStatus'] == 'COMPLETED':
            print(
                "refresh completed. RowsIngested {0}, RowsDropped {1}, IngestionTimeInSeconds {2}, IngestionSizeInBytes {3}".format(
                    response['Ingestion']['RowInfo']['RowsIngested'],
                    response['Ingestion']['RowInfo']['RowsDropped'],
                    response['Ingestion']['IngestionTimeInSeconds'],
                    response['Ingestion']['IngestionSizeInBytes']))
            return True
        else:
            raise Exception("refresh failed! - status {0}".format(response['Ingestion']['IngestionStatus']))

class GlueOperator(BaseOperator):
    """An operator to run AWS Glue jobs

        :param job_name: the name of the glue job
        :type job_name: str
        :param glue_role: the role for the Glue job to execute as
        :type glue_role: str
        :param job_config: the config for the Glue job
        :type job_config: dict
        :param python_file_location: s3 location for the script to be located
        :type python_file_location: str
        :param python_local_path: the local path to the script
        :type python_local_path: str
        :param python_version: the version to run of Python on Glue
        :type python_version: int
        :param glue_max_capacity: the capacity for the Glue job to run with
        :type glue_max_capacity: int
        :param validation_spec: whether this is a validation_spec job or not
        :type validation_spec: bool
        :param override_dict: dictionary overriding the ssm dictionary
        """

    def __init__(self,
                 job_name,
                 glue_role,
                 job_config,
                 python_file_location,
                 python_local_path,
                 python_version=3,
                 glue_max_capacity=10,
                 max_concurrent_runs=10,
                 validation_spec=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.job_name = job_name
        self.glue_role = glue_role
        self.job_config = job_config
        self.python_local_path = python_local_path
        self.python_file_location = python_file_location
        self.python_version = python_version
        self.glue_max_capacity = glue_max_capacity
        self.validation_spec = validation_spec
        self.max_concurrent_runs = max_concurrent_runs

    def upload_file(self,
                    s3_client: boto3.client,
                    python_local_path: str,
                    scripts_bucket: str,
                    python_file_location: str):
        """Pass through function to upload file to AWS

        :param s3_client: the client which connects to s3
        :param python_local_path: the local path to the Python file
        :param scripts_bucket: the location to save the Python file on S3
        :param python_file_location: the location to save it on the bucket
        """
        s3_client.upload_file(python_local_path, scripts_bucket, python_file_location)

    def create_job(self,
                   glue: boto3.client,
                   job_name: str,
                   python_file_location: str,
                   scripts_bucket: str = os.getenv("ETL_SCRIPTS_BUCKET"), ):
        """Creates an AWS Glue job

        :param glue: Boto3 client for glue
        :param job_name: the name of the job to create
        :param python_file_location: the location of the python file
        :param scripts_bucket: the bucket for the Python script
        :param role: the role to run the job as
        """
        try:
            job = glue.create_job(
                Name=job_name,
                Role=self.glue_role,
                MaxCapacity=float(self.glue_max_capacity),
                ExecutionProperty={
                    'MaxConcurrentRuns': int(self.max_concurrent_runs)
                },
                Command={
                    'Name': 'glueetl',
                    'ScriptLocation': f's3://{scripts_bucket}/{python_file_location}',
                    'PythonVersion': '3'
                },
                DefaultArguments={
                    '--enable-metrics': ''
                },
                GlueVersion='2.0'
                # Timeout=self.glue_timeout,
                # MaxCapacity=self.glue_max_capacity
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists' or e.response['Error'][
                'Code'] == 'IdempotentParameterMismatchException':
                print('Job already exists. Updating the Glue Job.')
                job = glue.update_job(
                    JobName=job_name,
                    JobUpdate=dict(
                        Role=self.glue_role,
                        MaxCapacity=float(self.glue_max_capacity),
                        Command=dict(
                            Name='glueetl',
                            ScriptLocation=f's3://{scripts_bucket}/{python_file_location}',
                            PythonVersion='3'
                        ),
                        ExecutionProperty={
                            'MaxConcurrentRuns': int(self.max_concurrent_runs)
                        },
                        DefaultArguments={
                            '--enable-metrics': ''
                        },
                        GlueVersion='2.0'
                    )
                )
            else:
                raise e
        return job

    def execute(self, context):

        # Printing operator configuration

        # Upload file to s3 for use
        s3 = boto3.client(service_name='s3')
        s3_path = '/'.join(self.python_file_location.replace('s3://', '').split('/')[1:])
        self.upload_file(s3,
                         self.python_local_path,
                         os.getenv('ETL_SCRIPTS_BUCKET'),
                         s3_path)

        glue = boto3.client(service_name='glue')
        job_name = self.job_name + f'-{os.getenv("ENVIRONMENT")}'
        job = self.create_job(glue,
                              job_name,
                              s3_path)

        from uuid import uuid4
        job_response_key = str(uuid4())

        run_args = dict(**{'--job_response_key': job_response_key},
                        **{'--' + k: v for k, v in self.job_config.items()},
                        **{'--environment': os.getenv("ENVIRONMENT")},
                        **{'--glue_response_bucket': os.getenv("GLUE_RESPONSE_BUCKET")})

        print(f'Running with args: {run_args}')

        job_run = glue.start_job_run(JobName=job_name,
                                     Arguments=run_args)

        while True:
            jr = glue.get_job_run(JobName=job_name, RunId=job_run['JobRunId'])
            status = jr['JobRun']['JobRunState']
            if status == 'SUCCEEDED':
                break
            elif status == 'FAILED':
                pprint(f'Last status update for job:\n{jr}')
                raise Exception(f'Glue Job: {job_run["JobRunId"]} failed.')
            elif status == 'STOPPED':
                pprint(f'Last status update for job:\n{jr}')
                raise Exception(f'Glue Job: {job_run["JobRunId"]} stopped.')
            else:
                print(
                    f'Job: {job_run["JobRunId"]} has not hit a terminal status.. Sleeping 30.\n Current status: {status}')
                sleep(30)

        # Job must be done, get results
        file = s3.get_object(Bucket=os.getenv("GLUE_RESPONSE_BUCKET"),
                             Key=f'{job_response_key}.json')
        contents = file['Body'].read()

        if self.validation_spec:
            resp_data = json.loads(contents)
            assert resp_data['status'] == 'success'

        return contents
