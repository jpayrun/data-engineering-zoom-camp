import os
from datetime import datetime

import pyarrow

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025,2,5),
    "depends_on_past": False,
    "retries": 1,
}

local_workflow = DAG(
    dag_id="LocalIngestionDag",
    schedule_interval="0 6 2 * *",
    default_args=default_args
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

def hello():
    print("Hello World!")
    return "I Ran!"

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
# Updating the execution date using jinja to the templates to Year Month
# URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
# OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output{{execution_date.strftime(\'%Y-%m\')}}.parquet'

# Removing jinja for testing since no 2025 data
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_2024-01.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output2024-01.parquet'

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")

with local_workflow:

    wget_task = BashOperator(
        task_id = 'curl',
        bash_command = f'curl -sS {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = PythonOperator(
        task_id = 'ingest_task',
        python_callable=ingest_callable,
        op_kwargs = {
            'table_name': "yellow_taxi_trips", 
            "password": PG_PASSWORD,
            "host": PG_HOST, 
            "user": PG_USER, 
            "port": PG_PORT, 
            "db" : PG_DATABASE,
            "csv_file": OUTPUT_FILE_TEMPLATE,
        }
    )

    wget_task >> ingest_task
