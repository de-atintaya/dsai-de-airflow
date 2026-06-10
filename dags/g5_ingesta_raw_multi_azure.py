from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta
from pathlib import Path
import csv


CONTAINER_NAME = "datalake"
WASB_CONN_ID = "utec_blob_storage"

TABLES = [
    {
        "name": "clientes",
        "local_file_path": "/opt/airflow/data/clientes_g5.csv",
        "blob_name": "raw/airflow2/G5/clientes/clientes_g5.csv",
    },
    {
        "name": "productos",
        "local_file_path": "/opt/airflow/data/productos_g5.csv",
        "blob_name": "raw/airflow2/G5/productos/productos_g5.csv",
    },
    {
        "name": "ventas",
        "local_file_path": "/opt/airflow/data/ventas_transacciones_g5.csv",
        "blob_name": "raw/airflow2/G5/ventas/ventas_transacciones_g5.csv",
    },
]


default_args = {
    "owner": "grupo_5",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="g5_ingesta_raw_multi_azure",
    description="Pipeline multiarchivo del Grupo 5 para cargar clientes, productos y ventas hacia la capa Raw en Azure Data Lake.",
    default_args=default_args,
    start_date=datetime(2026, 6, 1, tzinfo=timezone("America/Lima")),
    schedule=None,
    catchup=False,
    tags=["azure", "raw", "grupo_5", "multiarchivo"],
)
def upload_multi_dag():

    @task
    def validar_archivo(local_file_path: str) -> str:
        file_path = Path(local_file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"No existe el archivo: {local_file_path}")

        if file_path.suffix.lower() != ".csv":
            raise ValueError(f"El archivo no tiene extensión CSV: {local_file_path}")

        if file_path.stat().st_size == 0:
            raise ValueError(f"El archivo está vacío: {local_file_path}")

        with file_path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.reader(file)
            header = next(reader, None)
            first_row = next(reader, None)

        if not header:
            raise ValueError(f"El archivo no tiene cabecera: {local_file_path}")

        if not first_row:
            raise ValueError(f"El archivo no tiene registros de datos: {local_file_path}")

        print(f"Archivo validado correctamente: {local_file_path}")
        print(f"Columnas detectadas: {header}")

        return local_file_path

    @task
    def preparar_ruta_raw(blob_name: str) -> str:
        new_blob_name = add_date_suffix(blob_name)
        print(f"Ruta Raw generada: {new_blob_name}")
        return new_blob_name

    @task
    def cargar_a_raw(local_file_path: str, blob_name: str) -> None:
        upload_to_adls(
            local_file_path=local_file_path,
            container_name=CONTAINER_NAME,
            blob_name=blob_name,
            wasb_conn_id=WASB_CONN_ID,
        )

        print(f"Archivo cargado correctamente a {CONTAINER_NAME}/{blob_name}")

    for table in TABLES:
        archivo_validado = validar_archivo.override(
            task_id=f"validar_{table['name']}"
        )(table["local_file_path"])

        ruta_raw = preparar_ruta_raw.override(
            task_id=f"preparar_ruta_{table['name']}"
        )(table["blob_name"])

        carga = cargar_a_raw.override(
            task_id=f"cargar_{table['name']}"
        )(archivo_validado, ruta_raw)

        archivo_validado >> ruta_raw >> carga


dag = upload_multi_dag()