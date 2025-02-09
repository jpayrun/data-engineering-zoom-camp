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

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

VEHICLE = 'fhv'
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
DATA_FILE = VEHICLE + '_tripdata_{{logical_date.strftime(\'%Y-%m\')}}.parquet'
URL = URL_PREFIX + DATA_FILE

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

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id=f"data_ingestion_gcp_{VEHICLE}_dag_fixed",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 12, 2),
    schedule_interval='@monthly',
    default_args=default_args,
    # Set to True to catchup
    catchup=True,
    # Turnoff to run multiple runs
    # max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {URL} > {path_to_local_home}/{DATA_FILE}"
    )


    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{VEHICLE}/{DATA_FILE}",
            "local_file": f"{path_to_local_home}/{DATA_FILE}",
        },
    )

    # Set up with Bigquery
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": VEHICLE + "_tripdata_{{logical_date.strftime(\'%Y-%m\')}}",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{VEHICLE}/{DATA_FILE}"],
            },
        },
    )

    download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task