"""Ingesta de archivos de zonas de pesca del grupo G202 a ADLS Raw."""

from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


LOCAL_DIRECTORY = Path("/opt/airflow/data/flt_zonas/incoming")
AZURE_CONN_ID = "azure_blob_storage"
CONTAINER_NAME = "airflow"
RAW_PREFIX = "raw/G202/flt_zonas"

FILES = {
    "vms_2021_w2.csv": {"vessel_id", "timestamp", "latitude", "longitude", "speed"},
    "esfuerzo_gfw_2021.csv": {"cell_lat", "cell_lon", "fishing_hours", "date"},
    "puertos.csv": {"puerto_id", "puerto_nombre", "latitude", "longitude"},
}

default_args = {
    "owner": "g202",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def validate_local_files() -> list[str]:
    """Comprueba que los CSV existan, tengan datos y contengan el esquema mínimo."""
    valid_files = []

    for file_name, required_columns in FILES.items():
        file_path = LOCAL_DIRECTORY / file_name
        if not file_path.is_file():
            raise AirflowException(f"No existe el archivo local: {file_path}")

        with file_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            columns = {column.strip().lower() for column in (reader.fieldnames or [])}
            if not required_columns.issubset(columns):
                missing = sorted(required_columns - columns)
                raise AirflowException(f"{file_name}: faltan columnas {missing}")
            if next(reader, None) is None:
                raise AirflowException(f"{file_name}: el archivo no contiene registros")

        valid_files.append(str(file_path))

    return valid_files


def upload_files_to_raw(ti, ds_nodash: str) -> None:
    """Carga los CSV validados en el contenedor Raw conservando el archivo fuente."""
    valid_files = ti.xcom_pull(task_ids="validate_local_files") or []
    if not valid_files:
        raise AirflowException("No se recibieron archivos validados")

    hook = WasbHook(wasb_conn_id=AZURE_CONN_ID)
    for file_name in valid_files:
        file_path = Path(file_name)
        source = (
            "esfuerzo_gfw"
            if file_path.name.startswith("esfuerzo_gfw")
            else file_path.stem.split("_")[0]
        )
        blob_name = f"{RAW_PREFIX}/{source}/{ds_nodash}/{file_path.name}"
        hook.load_file(
            file_path=str(file_path),
            container_name=CONTAINER_NAME,
            blob_name=blob_name,
            overwrite=True,
        )
        print(f"Archivo cargado: {CONTAINER_NAME}/{blob_name}")


with DAG(
    dag_id="g202_flt_zonas_ingesta",
    description="Ingesta de flt_zonas desde archivos locales hacia Azure Data Lake Raw",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["g202", "flt_zonas", "azure", "raw"],
) as dag:
    validate = PythonOperator(
        task_id="validate_local_files",
        python_callable=validate_local_files,
    )

    upload = PythonOperator(
        task_id="upload_files_to_raw",
        python_callable=upload_files_to_raw,
    )

    validate >> upload
