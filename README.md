Side project to ETL mlb data

Useful commands:
Activate venv - source airflowenv/bin/activate
Activate airflow - airflow standalone
create user - airflow users create -u airflow -f airflow -l airflow -r Admin -e airflow@gmail.com




docker-compose up airflow-init
docker-compose up -d

start up airflow in docker aws: docker-compose -f prod-docker-compose.yaml up -d

docker-compose down -v

show constainers health: docker ps

docker build . --tag extending_airflow:latest

https://github.com/mikestack15/orangutan-stem/wiki/Activity-1:-Open-Weather-Map-API-Data-Pipeline

