FROM apache/airflow:latest-python3.11
COPY requirments.txt /requirments.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirments.txt