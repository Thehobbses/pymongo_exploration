import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import datetime

from airflow import models
from airflow.operators import bash


EMAIL = os.environ['email']
WORKING_DIRECTORY = os.environ['working_dir']

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'Jack Hobbs',
    'depends_on_past': False,
    'email': [EMAIL],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

with models.DAG(
        'airflow_pipeline',
        catchup=False,
        default_args=default_args,
        description='simple pipeline from the Library of Congress',
        schedule_interval=datetime.timedelta(days=1)) as dag:

    # Print the dag_run id from the Airflow logs
    push_to_mongo = bash.BashOperator(
        task_id='push_to_mongo',
        bash_command=f'{WORKING_DIRECTORY}/PushToMongoDB.py',
        dag=dag)

    pull_from_mongo = bash.BashOperator(
        task_id='pull_from_mongo',
        depends_on_past=False,
        bash_command=f'{WORKING_DIRECTORY}/PullFromMongoDB.py',
        retries=3,
        dag=dag)

    visualize_mongo_data = bash.BashOperator(
        task_id='visualize_mongo_data',
        depends_on_past=False,
        bash_command=f'{WORKING_DIRECTORY}/VisualizationHandling.py',
        retries=3,
        dag=dag)


# Ordering the DAG. '>>' initiates task 2 after task 1 is finished
push_to_mongo >> pull_from_mongo >> visualize_mongo_data