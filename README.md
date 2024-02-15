Side project to ETL mlb data

Useful commands:
Activate venv - source airflowenv/bin/activate
Activate airflow - airflow standalone
create user - airflow users create -u airflow -f airflow -l airflow -r Admin -e airflow@gmail.com

airflow webserver &

airflow scheduler

kill $(ps -o ppid= -p $(cat ~/airflow/airflow-webserver.pid))

kill $(cat ~/airflow/airflow-scheduler.pid)