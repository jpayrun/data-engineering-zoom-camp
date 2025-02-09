import os
import datetime as dt
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd
import pyarrow

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file) -> None:
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "start_date": dt.datetime(2019, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

GREEN_SCHEMA = [
    {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "lpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
    {"name": "RatecodeID", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "passenger_count", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "ehail_fee", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "payment_type", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trip_type", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
]

YELLOW_SCHEMA = [
    {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "tpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "tpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "passenger_count", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "RatecodeID", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
    {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "payment_type", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "airport_fee", "type": "FLOAT", "mode": "NULLABLE"},
]

SCHEMA = {'yellow': YELLOW_SCHEMA, 'green': GREEN_SCHEMA}

def yellow(file: str) -> None:
    (pd.read_parquet(file).
      assign(airport_fee = lambda df_a: df_a.airport_fee.fillna(0.0)).
      assign(airport_fee = lambda df_a: df_a.airport_fee.astype('float64')).
      to_parquet(file)
      )

def green(file: str) -> None:
    (pd.read_parquet(file).
      assign(ehail_fee = lambda df_a: df_a.ehail_fee.fillna(0.0)).
      assign(ehail_fee = lambda df_a: df_a.ehail_fee * 1.0).
      assign(ehail_fee = lambda df_a: df_a.ehail_fee.astype('float64')).
      to_parquet(file))

fix_function = {
    'yellow': yellow,
    'green': green
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcp_yellow_green_taxi_dag",
    start_date=dt.datetime(2019, 1, 1),
    # end_date=dt.datetime(2019, 4, 1),
    end_date=dt.datetime(2020, 12, 1),
    schedule_interval='@monthly',
    default_args=default_args,
    # Set to True to catchup
    catchup=True,
    # Turnoff to run multiple runs
    # max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for color in ['yellow', 'green']:

        URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
        DATA_FILE = color + '_tripdata_{{logical_date.strftime(\'%Y-%m\')}}.parquet'
        URL = URL_PREFIX + DATA_FILE

        download_dataset_task = BashOperator(
            task_id=f"download_{color}_taxi_dataset_task",
            bash_command=f"curl -sSL {URL} > {path_to_local_home}/{DATA_FILE}"
        )

        fix_for_upload = PythonOperator(
            task_id=f"{color}_fixing_columns",
            python_callable=fix_function[color],
            op_kwargs={
                "file":f"{path_to_local_home}/{DATA_FILE}"
            }
        )

        local_to_gcs_task = PythonOperator(
            task_id=f"{color}_taxi_local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{color}_taxi/{DATA_FILE}",
                "local_file": f"{path_to_local_home}/{DATA_FILE}",
            },
        )

        # Set up with Bigquery
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"{color}_taxi_bigquery_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": color + "_tripdata_{{logical_date.strftime(\'%Y-%m\')}}",
                },
                # "schema": SCHEMA[color],
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "schema_fields": SCHEMA[color],
                    "sourceUris": [f"gs://{BUCKET}/raw/{color}_taxi/{DATA_FILE}"],
                },
            },
        )

        download_dataset_task >> fix_for_upload >> local_to_gcs_task >> bigquery_external_table_task
