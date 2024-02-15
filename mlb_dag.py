from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from mlbapi import  player_lookup

default_args = {
    'owner': 'troy',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 15),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'mlb_dag',
    default_args=default_args,
    description='first dag for mlb project',
)

run_etl = PythonOperator(
    task_id='player_lookup',
    python_callable=player_lookup,
    dag=dag, 
)

run_etl