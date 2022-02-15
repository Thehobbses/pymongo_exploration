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
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
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
    bash_command=f'{WORKING_DIRECTORY}/VisualizationHandling.py',
    retries=3,
    dag=dag)

# Ordering the DAG. '>>' initiates task 2 after task 1 is finished
push_to_mongo >> pull_from_mongo >> visualize_mongo_data
