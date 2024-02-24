from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime
from mlbapi import player_lookup, upload_bravesStats

default_args = {
    'owner': 'troy',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': None,
}

with DAG(
    default_args=default_args,
    dag_id='mlb_dag',
) as dag:
    
    get_playerdata = PythonOperator(
        task_id='player_lookup',
        python_callable=player_lookup,
    )

    # Task to upload DataFrame to S3
    upload_bravesStats = PythonOperator(
        task_id='upload_bravesStats',
        python_callable=upload_bravesStats,
        provide_context=True,
    )

get_playerdata >> upload_bravesStats