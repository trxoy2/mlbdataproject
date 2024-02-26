from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from mlbapi import lookup_ozzie, lookup_ronald, lookup_olson, lookup_riley, combine_players, upload_bravesStats, transform_bravesStats

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
    
    with TaskGroup('collect_players') as collect_players:
        lookup_ozzie = PythonOperator(
        task_id='lookup_ozzie',
        python_callable=lookup_ozzie,
        )

        lookup_ronald = PythonOperator(
        task_id='lookup_ronald',
        python_callable=lookup_ronald,
        )

        lookup_olson = PythonOperator(
        task_id='lookup_olson',
        python_callable=lookup_olson,
        )

        lookup_riley = PythonOperator(
        task_id='lookup_riley',
        python_callable=lookup_riley,
        )

    combine_players = PythonOperator(
        task_id='combine_players',
        python_callable=combine_players,
    )

    transform_bravesStats = PythonOperator(
        task_id='transform_bravesStats',
        python_callable=transform_bravesStats,
    )

    upload_bravesStats = PythonOperator(
        task_id='upload_bravesStats',
        python_callable=upload_bravesStats,
        provide_context=True,
    )

collect_players >> combine_players >> transform_bravesStats >> upload_bravesStats