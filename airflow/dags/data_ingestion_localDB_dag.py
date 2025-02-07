import os
from datetime import datetime

import pyarrow

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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
URL_TEMPLATE = f'{URL_PREFIX}/yellow_tripdata_2024-01.parquet'


with local_workflow:

    wget_task = BashOperator(
        task_id = 'curl',
        # bash_command = f'curl -sS {URL_TEMPLATE} > {AIRFLOW_HOME}/output.parquet'
        # Formats the date into Year Month
        bash_command = 'ehco "{{ execution_date.strftime(\'%Y-%m\')}}"'
    )

    # ingest_task = PythonOperator(
    #     task_id='ingest',
    #     python_callable=hello
    # )

    ingest_task = BashOperator(
        task_id='ingest',
        bash_command=f'ls {AIRFLOW_HOME}'
    )

    wget_task >> ingest_task
