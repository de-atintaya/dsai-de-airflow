from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta

LOCAL_FILE_PATH = "/opt/airflow/data/ventas_transacciones_g5.csv"
CONTAINER_NAME = "datalake"
BLOB_NAME = "raw/airflow2/G5/ventas_transacciones_g5.csv"

default_args = {
    "owner": "grupo_5",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="g5_ingesta_raw_azure",
    description="Pipeline de ingesta del Grupo 5: mueve un archivo local simulado hacia la capa Raw en Azure Data Lake.",
    default_args=default_args,
    start_date=datetime(2026, 6, 1, tzinfo=timezone("America/Lima")),
    schedule=None,
    catchup=False,
    tags=["azure", "raw", "grupo_5", "ingesta"],
)
def upload_dag():

    @task
    def cargar_archivo_raw():
        new_blob_name = add_date_suffix(BLOB_NAME)

        upload_to_adls(
            local_file_path=LOCAL_FILE_PATH,
            container_name=CONTAINER_NAME,
            blob_name=new_blob_name,
            wasb_conn_id="utec_blob_storage",
        )

    cargar_archivo_raw()

dag = upload_dag()
