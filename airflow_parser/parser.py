import io
from datetime import datetime
from os import walk, getenv

import jinja2
import yaml
from airflow.models import DAG
from airflow.models.baseoperator import chain

from airflow_parser.providers.carte.operators.carte import CarteExecuteJobOperator, CarteCheckJobOperator, \
    CarteGenerateRuntimeParametersOperator


class DAGGenerator:
    def __init__(self,
                 control_file_path: str):
        self.control_file_path = control_file_path

    def generate_dags(self):
        control_file_path = getenv('CONTROL_FILE_PATH', '/opt/airflow/control_files/')

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

        DEFAULT_ARGS = {
            'owner': 'BioIQ',
            'start_date': datetime(2020, 1, 1),  # Abitrary date in the past, won't matter since catchup=False
            'depends_on_past': False,
            # 'on_failure_callback': on_failure_callback
        }

        for flow_spec in flow_specs:
            with DAG(dag_id=flow_spec['name'],
                     schedule_interval=flow_spec.get('schedule', None),
                     catchup=False,
                     default_args=DEFAULT_ARGS,
                     max_active_runs=1) as dag:
                ops = []
                args_list = set()
                for step_spec in flow_spec['flow']:
                    step = step_specs[step_spec]
                    if 'runtime_parameters' in step:
                        for item in step['runtime_parameters']:
                            args_list.add(item)
                if len(args_list) > 0:
                    op = CarteGenerateRuntimeParametersOperator(task_id='generate_runtime_params',
                                                                args_list=list(args_list))
                    ops.append(op)
                for step_spec in flow_spec['flow']:
                    step = step_specs[step_spec]
                    etl_platform = step['etl_platform']
                    if etl_platform == 'pentaho':
                        carte_user = getenv('CARTE_USER')
                        carte_password = getenv('CARTE_PASSWORD')
                        host = getenv('CARTE_HOST')
                        job_name = step['etl_platform_filename']
                        execute_task_id = f'execute_carte_{step["name"]}'
                        op = CarteExecuteJobOperator(task_id=execute_task_id,
                                                     repository_name=step['parameters']['repository_name'],
                                                     job_name='/'.join(step['etl_platform_filename'].split('/')[1:]).replace('.kjb', ''),
                                                     carte_user=carte_user,
                                                     carte_password=carte_password,
                                                     carte_level='Basic',
                                                     host=host)
                        ops.append(op)
                        op = CarteCheckJobOperator(task_id=f'check_carte_{step["name"]}',
                                                   carte_user=carte_user,
                                                   job_name=step['etl_platform_filename'].split('/')[-1].replace('.kjb', ''),
                                                   carte_password=carte_password,
                                                   job_execute_task_id=execute_task_id,
                                                   host=host)
                        ops.append(op)
                chain(*ops)
                print(dag)
                globals()[flow_spec["name"]] = dag
