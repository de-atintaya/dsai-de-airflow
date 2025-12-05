from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta
from pathlib import Path
import os

#LOCAL_FILE_PATH = "/opt/airflow/data/sample.txt"
SOURCE_DIR = "/opt/airflow/data/final"   # <-- aquí está tu carpeta 'parcial'
DEST_PREFIX = "raw/airflow/G10/final"           # raíz destino en el contenedor
USE_DATE_SUFFIX_ON_ROOT = False  # pon True si quieres raw/airflow/G10/parcial_YYYYMMDD/...


CONTAINER_NAME = "datalake" # airflow
WASB_CONN_ID = "utec_blob_storage"
#BLOB_NAME = "raw/airflow/G0/archivo_subido.txt"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g10_prjparcial",
    description="Uploads a local dir to Azure Blob Storage ",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 12 * * 1", # Runs every monday at 12:00 local time (GMT-5)
    catchup=False,
    tags=["utec", "blob", "upload", "folder"],
)


def upload_dag():

    @task
    def call_upload():
        for root, _, files in os.walk(SOURCE_DIR):
            for fname in files:
                local_path = os.path.join(root, fname)
                rel_path = os.path.relpath(local_path, SOURCE_DIR).replace(os.sep, "/")
                blob_name = f"{DEST_PREFIX}/{rel_path}"

                # llama a tu helper (ya hace overwrite=True dentro)
                upload_to_adls(
                    local_file_path=local_path,
                    container_name=CONTAINER_NAME,
                    blob_name=blob_name,
                    wasb_conn_id=WASB_CONN_ID
                )

    call_upload()

dag = upload_dag()
