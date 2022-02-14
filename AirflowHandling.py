import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


EMAIL = os.environ['email']
WORKING_DIRECTORY = os.environ['working_dir']

default_args = {
    'owner': 'Jack Hobbs',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': [EMAIL],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2022, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# initiate a directed acyclic graph
dag = DAG(
    'airflow pipeline',
    default_args=default_args,
    description='simple pipeline from the Library of Congress',
    schedule_interval=timedelta(days=1))

# initiate tasks using py scripts as bash operators
push_to_mongo = BashOperator(
    task_id='push_to_mongo',
    bash_command=f'{WORKING_DIRECTORY}/PushToMongoDB.py',
    dag=dag)

pull_from_mongo = BashOperator(
    task_id='pull_from_mongo',
    depends_on_past=False,
    bash_command=f'{WORKING_DIRECTORY}/PullFromMongoDB.py',
    retries=3,
    dag=dag)

visualize_mongo_data = BashOperator(
    task_id='visualize_mongo_data',
    depends_on_past=False,
    bash_command=f'{WORKING_DIRECTORY}/VisualizeMongoDB.py',
    retries=3,
    dag=dag)


serve_commands = """
    lsof -i tcp:8008 | awk 'NR!=1 {print $2}' | xargs kill;
    python3 /usr/local/airflow/scripts/serve.py serve
    """

serve = BashOperator(
    task_id='serve',
    depends_on_past=False,
    bash_command=serve_commands,
    retries=3,
    dag=dag)

# Ordering the DAG. '>>' initiates task 2 after task 1 is finished
push_to_mongo >> pull_from_mongo >> visualize_mongo_data >> serve
