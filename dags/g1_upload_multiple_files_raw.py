from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime, timedelta
import os
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix

LOCAL_FOLDER_PATH = "/opt/airflow/data/local-server"
CONTAINER_NAME = "datalake"
WASB_CONN_ID = "utec_blob_storage"
BLOB_NAME = "raw/airflow/G6/"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g1_upload_multiple_files_raw",
    description="Uploads multiple local files to Azure Blob Storage with a date suffix.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 9 * * *",
    catchup=False,
    tags=["utec", "blob", "upload", "multiple_files"],
)
def upload_multiple_files_dag():

    @task
    def list_files():
        files = [
            f for f in os.listdir(LOCAL_FOLDER_PATH)
            if os.path.isfile(os.path.join(LOCAL_FOLDER_PATH, f))
        ]
        return files

    @task
    def upload_files(file_list: list):
        for file in file_list:
            local_path = os.path.join(LOCAL_FOLDER_PATH, file)
            blob_path = BLOB_NAME + add_date_suffix(file)
            upload_to_adls(
                local_file_path=local_path,
                container_name=CONTAINER_NAME,
                blob_name=blob_path,
                wasb_conn_id=WASB_CONN_ID
            )

    files = list_files()
    upload_files(files)

dag = upload_multiple_files_dag()
