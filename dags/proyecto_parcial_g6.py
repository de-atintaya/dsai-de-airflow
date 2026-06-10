from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta

#LOCAL_FILE_PATH = "/opt/airflow/data/mining_stdtravel.csv" 

FILES_TO_UPLOAD = [
    {
        "local_path": "/opt/airflow/data/mining_stdtravel.csv",
        "blob_name": "raw/airflow2/G6/archivo_subido_mining_stdtravel.csv"
    },
    {
        "local_path": "/opt/airflow/data/mining_pittruck.csv",
        "blob_name": "raw/airflow2/G6/archivo_subido_mining_pittruck.csv"
    }
]

CONTAINER_NAME = "datalake" 
BLOB_NAME = "raw/airflow2/G6/archivo_subido_mining_stdtravel.csv" 
WASB_CONN_ID = "utec_blob_storage"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g6_proyecto_parcial", # Nombre único para evitar conflictos en Airflow
    description="Pipeline de ingesta de datos a Azure Data Lake (Capa Raw) - Avance",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule=None, # Puedes cambiarlo a un cron si exigen programación automática
    catchup=False,
    tags=["utec", "datalake", "avance", "G6"],
)
def upload_avance_dag():

    @task
    def call_upload():
        for file in FILES_TO_UPLOAD:
            new_blob_name = add_date_suffix(file["blob_name"])
            
            upload_to_adls(
                local_file_path=file["local_path"],
                container_name=CONTAINER_NAME,
                blob_name=new_blob_name,
                wasb_conn_id=WASB_CONN_ID
            )

    call_upload()

# Instanciar el DAG
dag = upload_avance_dag()