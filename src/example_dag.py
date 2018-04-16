from airflow.operators import SparkLivyOperator
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': '2017-04-13',
    'email': ['ddevang93@gmail.com'],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG('sample_test', default_args=default_args, schedule_interval="0 11 * * *")

t1 = SparkLivyOperator(
    task_id='livy_op',
    user='depatel',
    password='patel',
    https_conn='',
    session_kind='',
    poll_interval='',
    op_kwargs={"file": "sample.jar"},
    dag=dag)