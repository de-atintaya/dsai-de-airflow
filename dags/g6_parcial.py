from airflow.decorators import dag, task
from pendulum import timezone
import pandas as pd
from os.path import exists
import logging

log = logging.getLogger(__name__)
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta

LOCAL_FILE_PATH = "/opt/airflow/data/equipos-utf.csv"
CONTAINER_NAME = "datalake" # airflow
WASB_CONN_ID = "utec_blob_storage"
BLOB_NAME = "raw/airflow/G6/equipos-utf.csv"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g6_parcial",
    description="Uploads a local file to Azure Blob Storage with a date suffix.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 12 * * 1", # Runs every monday at 12:00 local time (GMT-5)
    catchup=False,
    tags=["utec", "blob", "upload", "encoding-fix"],
)
def upload_dag():

    @task
    def prepare_csv_encoding(file_path: str):
        if not exists(file_path):
            log.warning(f"File not found: {file_path}. Skipping encoding preparation.")
            return file_path

        log.info(f"Attempting to read file: {file_path}")

        try:
            df = pd.read_csv(file_path, encoding='Windows-1252')
            log.info("Successfully read file using Windows-1252 encoding.")
        except UnicodeDecodeError:
            log.warning("Windows-1252 failed. Trying latin1.")
            try:
                df = pd.read_csv(file_path, encoding='latin1')
                log.info("Successfully read file using latin1 encoding.")
            except Exception as e:
                log.error(f"Failed to read file with Latin encodings. Error: {e}")
                raise
        df.to_csv(file_path, index=False, encoding='utf-8')
        log.info(f"File successfully re-encoded and saved as UTF-8 at: {file_path}")
        
        return file_path # Retorna la ruta del archivo limpio


    @task
    def call_upload():
        new_blob_name = add_date_suffix(BLOB_NAME)
        upload_to_adls(
            local_file_path=LOCAL_FILE_PATH,
            container_name=CONTAINER_NAME,
            blob_name=new_blob_name,
            wasb_conn_id = WASB_CONN_ID
            )
    prepared_path = prepare_csv_encoding(LOCAL_FILE_PATH)
    call_upload(prepared_path)

dag = upload_dag()
