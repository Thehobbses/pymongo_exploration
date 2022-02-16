# Python Modules
import datetime
import airflow
# from airflow import models
from airflow.operators.python import PythonOperator

# DAG modules
import MongoDBTools, PullFromMongoDB, PushToMongoDB, VisualizationHandling

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'Jack Hobbs',
    'depends_on_past': False,
    'email': 'jackhobbs97@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
    'schedule_interval': '@daily'
}

with airflow.models.DAG(
        'airflow_pipeline',
        catchup=False,
        default_args=default_args,
        description='simple pipeline from the Library of Congress',
        schedule_interval=datetime.timedelta(days=1)) as dag:

    # run scripts
    push_to_mongo = PythonOperator(
        task_id='push_to_mongo',
        python_callable=PushToMongoDB.main,
        dag=dag)

    pull_from_mongo = PythonOperator(
        task_id='pull_from_mongo',
        depends_on_past=False,
        python_callable=PullFromMongoDB.main,
        retries=3,
        dag=dag)

    visualize_mongo_data = PythonOperator(
        task_id='visualize_mongo_data',
        depends_on_past=False,
        python_callable=VisualizationHandling.main,
        retries=3,
        dag=dag)


# Ordering the DAG. '>>' initiates task 2 after task 1 is finished
push_to_mongo >> pull_from_mongo >> visualize_mongo_data
