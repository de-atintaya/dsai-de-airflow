from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta

LOCAL_FILE_PATH = "/opt/airflow/data/mining_stdtravel.csv" 
# 1. El contenedor ahora es "datalake" (No "airflow")
CONTAINER_NAME = "datalake" 
# 2. Guardar en raw/airflow2/G6/... (Importante usar airflow2 para diferenciarse)
BLOB_NAME = "raw/airflow2/G6/archivo_subido_mining_stdtravel.csv" 
# 3. Utilizar la conexión enviada para el datalake de UTEC
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
        # Añade la fecha al archivo para evitar sobreescritura (buenas prácticas)
        new_blob_name = add_date_suffix(BLOB_NAME)
        
        # Sube el archivo usando la nueva conexión y contenedor
        upload_to_adls(
            local_file_path=LOCAL_FILE_PATH,
            container_name=CONTAINER_NAME,
            blob_name=new_blob_name,
            wasb_conn_id=WASB_CONN_ID
        )

    call_upload()

# Instanciar el DAG
dag = upload_avance_dag()