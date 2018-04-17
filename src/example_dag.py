from airflow.operators import SparkLivyOperator
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2018,04,15),
    'end_date': datetime(2018,04,18),
    'email': ['depatel@starbucks.com'],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG('sample_test', default_args=default_args, schedule_interval="0 11 * * *")

t1 = SparkLivyOperator(
    task_id="livy_op",
    user="<user>",
    password="<password>",
    https_con="<cluster_livy>",
    session_kind="spark",
    poll_interval=60,
    op_kwargs={"file": "sample_assembly.jar", "className": "Main" },
    dag=dag)