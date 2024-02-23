Side project to ETL mlb data

Useful commands:
Activate venv - source airflowenv/bin/activate
Activate airflow - airflow standalone
create user - airflow users create -u airflow -f airflow -l airflow -r Admin -e airflow@gmail.com

airflow webserver &

airflow scheduler

kill $(ps -o ppid= -p $(cat ~/airflow/airflow-webserver.pid))

kill $(cat ~/airflow/airflow-scheduler.pid)

docker-compose up airflow-init

docker-compose up
docker-compose up airflow-init
docker-compose up -d

docker-compose down -v

docker ps

docker build . --tag extending_airflow:latest

https://github.com/mikestack15/orangutan-stem/wiki/Activity-1:-Open-Weather-Map-API-Data-Pipeline

http://ec2-3-141-19-82.us-east-2.compute.amazonaws.com:8080/