from airflow.decorators import dag, task
from pendulum import timezone
import pandas as pd
from os.path import exists
import logging

log = logging.getLogger(__name__)
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta

# --- Variables de Configuración ---
BASE_LOCAL_PATH = "/opt/airflow/data/" # La carpeta base
CONTAINER_NAME = "datalake" 
WASB_CONN_ID = "utec_blob_storage"

FILES_TO_UPLOAD = [
    "equipos_final.csv",
    "clientes_final.csv",
    "ventas_final.csv",
]

AZURE_BASE_BLOB_PATH = "raw/airflow/G6/"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g6_final",
    description="Uploads a local file to Azure Blob Storage with a date suffix.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 12 * * 1", # Runs every monday at 12:00 local time (GMT-5)
    catchup=False,
    tags=["utec", "blob", "upload", "encoding-fix"],
)
def upload_dag():

    @task
    def prepare_file_list(files_to_upload: list[str], base_local_path: str, azure_base_blob_path: str):
        """Prepara los parámetros de carga para cada archivo."""
        upload_params = []
        for file_name in files_to_upload:
            upload_params.append({
                "local_file_path": base_local_path + file_name,
                "blob_name_base": azure_base_blob_path + file_name,
            })
        return upload_params # Retorna una lista de diccionarios de parámetros

    @task
    def call_upload(params: dict):
        """
        Tarea que se ejecutará N veces, una por cada elemento mapeado.
        'params' contendrá el diccionario de un único archivo.
        """
        # Desempaquetar los parámetros
        local_path = params["local_file_path"]
        blob_base = params["blob_name_base"]
        
        new_blob_name = add_date_suffix(blob_base) 
        
        log.info(f"💾 Cargando archivo local: {local_path}")
        log.info(f"☁️ Blob de destino en Azure: {CONTAINER_NAME}/{new_blob_name}")
        
        upload_to_adls(
            local_file_path=local_path,
            container_name=CONTAINER_NAME,
            blob_name=new_blob_name,
            wasb_conn_id=WASB_CONN_ID
        )
        log.info(f"✅ Carga de {local_path} completada!")
        
    
    upload_list = prepare_file_list(
        files_to_upload=FILES_TO_UPLOAD,
        base_local_path=BASE_LOCAL_PATH,
        azure_base_blob_path=AZURE_BASE_BLOB_PATH
    )

    upload_task = call_upload.expand(params=upload_list) 

dag = upload_dag()