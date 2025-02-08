import datetime as dt
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

DATA_FILE = "taxi_zone_lookup.csv"
PARQUET_FILE = DATA_FILE[:-4] + '.parquet'
URL = "https://d37ci6vzurychx.cloudfront.net/misc/" + DATA_FILE

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

def format_to_parquet(src_file) -> None:
    # if not src_file.endswith('.csv'):
    #     logging.error("Can only accept source files in CSV format, for the moment")
    #     return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "start_date": dt.datetime(2019, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="Zones_Upload",
    schedule_interval='@yearly',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
) as dag:
    download_zones = BashOperator(
        task_id = "Download_Zones",
        bash_command=f'curl {URL} > {path_to_local_home}/{DATA_FILE}'
    )

    to_parquet = PythonOperator(
        task_id='to_parquet',
        python_callable=format_to_parquet,
        op_kwargs={
            'src_file':f'{path_to_local_home}/{DATA_FILE}'
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/zone/{PARQUET_FILE}",
            "local_file": f"{path_to_local_home}/{PARQUET_FILE}",
        },
    )


    # Set up with Bigquery
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": 'Zones',
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/zone/{PARQUET_FILE}"],
            },
        },
    )

    download_zones >> to_parquet >> local_to_gcs_task >> bigquery_external_table_task
