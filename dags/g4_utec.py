from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta
import os

# Directorio local donde Airflow montará los CSV
LOCAL_DIR = "/opt/airflow/data"

# Lista de ficheros a procesar
DATASETS = [
    "accounts.csv",
    "branches.csv",
    "customers.csv",
    "transactions.csv"
]

# Contenedor y prefijo en ADLS
CONTAINER_NAME = "datalake"           # igual que en tu template
WASB_CONN_ID    = "utec_blob_storage" # la Connection que configuraste en Airflow
BLOB_BASE_PATH  = "raw/airflow/G4"    # ruta destino dentro del contenedor

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g4_utec",
    description="Sube los 4 CSV de /opt/airflow/data a ADLS en raw/airflow/G4 con sufijo de fecha",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 12 * * 1",  # cada lunes al mediodía Bogotá
    catchup=False,
    tags=["utec", "blob", "upload"],
)
def upload_csvs_dag():

    @task
    def upload_file(dataset_filename: str):
        """
        Sube un CSV desde LOCAL_DIR a ADLS usando tu helper upload_to_adls,
        aplicando add_date_suffix y la Connection WASB_CONN_ID.
        """
        local_path = os.path.join(LOCAL_DIR, dataset_filename)
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"No existe: {local_path}")

        # Construcción del blob con fecha
        base_blob = f"{BLOB_BASE_PATH}/{dataset_filename}"
        blob_name = add_date_suffix(base_blob)

        # Ejecución de la subida
        upload_to_adls(
            local_file_path=local_path,
            container_name=CONTAINER_NAME,
            blob_name=blob_name,
            wasb_conn_id=WASB_CONN_ID
        )

        print(f"✅ {dataset_filename} → {CONTAINER_NAME}/{blob_name}")
        return blob_name

    # Generamos una tarea por cada CSV
    for fname in DATASETS:
        upload_file(fname)

# Instanciamos el DAG
dag = upload_csvs_dag()
