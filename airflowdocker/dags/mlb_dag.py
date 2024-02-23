from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from mlbapi import player_lookup

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

with DAG(
    default_args=default_args,
    dag_id='mlb_dag',
) as dag:
    
    get_playerdata = PythonOperator(
        task_id='player_lookup',
        python_callable=player_lookup,
    )

get_playerdata