from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import shutil

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


INCOMING_DIR = "/opt/airflow/data/vms/incoming"
PROCESSED_DIR = "/opt/airflow/data/vms/processed"
QUARANTINE_DIR = "/opt/airflow/data/vms/quarantine"
STAGING_DIR = "/opt/airflow/data/vms/staging"

AZURE_CONN_ID = "azure_blob_storage"
CONTAINER_NAME = "airflow"


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def get_csv_files():
    incoming_path = Path(INCOMING_DIR)
    return list(incoming_path.glob("*.csv"))


def extract_data(**context):
    files = get_csv_files()

    if not files:
        raise AirflowException("No se encontraron archivos CSV en la carpeta incoming.")

    extracted_files = []

    for file_path in files:
        try:
            df = pd.read_csv(file_path)

            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                df = df.dropna(subset=["timestamp"])
                df = df.sort_values("timestamp")

            clean_path = str(Path(STAGING_DIR) / (file_path.stem + "_clean.csv"))
            df.to_csv(clean_path, index=False)

            extracted_files.append(clean_path)

        except Exception:
            quarantine_path = Path(QUARANTINE_DIR) / file_path.name
            shutil.move(str(file_path), str(quarantine_path))
            raise AirflowException(f"Archivo inválido enviado a cuarentena: {file_path.name}")

    context["ti"].xcom_push(key="clean_files", value=extracted_files)


def validate_schema(**context):
    expected_columns = {"vessel_id", "timestamp", "latitude", "longitude", "speed"}

    clean_files = context["ti"].xcom_pull(
        task_ids="extract_data",
        key="clean_files"
    )

    valid_files = []

    for file_name in clean_files:
        df = pd.read_csv(file_name)

        columns = set(df.columns)

        if not expected_columns.issubset(columns):
            quarantine_path = Path(QUARANTINE_DIR) / Path(file_name).name
            shutil.move(file_name, str(quarantine_path))
            raise AirflowException(f"Esquema inválido en archivo: {file_name}")

        df = df[
            (df["latitude"].between(-90, 90)) &
            (df["longitude"].between(-180, 180)) &
            (df["speed"] >= 0)
        ]

        if df.empty:
            quarantine_path = Path(QUARANTINE_DIR) / Path(file_name).name
            shutil.move(file_name, str(quarantine_path))
            raise AirflowException(f"Archivo sin registros válidos: {file_name}")

        validated_path = str(Path(STAGING_DIR) / Path(file_name).name.replace("_clean.csv", "_validated.csv"))
        df.to_csv(validated_path, index=False)

        valid_files.append(validated_path)

    context["ti"].xcom_push(key="valid_files", value=valid_files)


def upload_to_adl(**context):
    valid_files = context["ti"].xcom_pull(
        task_ids="validate_schema",
        key="valid_files"
    )

    execution_date = context["ds"]
    year, month, day = execution_date.split("-")

    hook = WasbHook(wasb_conn_id=AZURE_CONN_ID)

    for file_name in valid_files:
        local_path = Path(file_name)
        blob_name = f"raw/vms/{year}/{month}/{day}/{local_path.name}"

        hook.load_file(
            file_path=str(local_path),
            container_name=CONTAINER_NAME,
            blob_name=blob_name,
            overwrite=True,
        )

        print(f"Archivo cargado a Azure: {CONTAINER_NAME}/{blob_name}")


def notify_failure(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    print(f"Fallo en DAG {dag_id}, tarea {task_id}. Revisar logs.")


with DAG(
    dag_id="dag_vms_ingestion",
    description="Pipeline diario de ingesta de archivos VMS hacia Azure Data Lake Raw",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    on_failure_callback=notify_failure,
    tags=["vms", "azure", "raw", "ingestion"],
) as dag:

    check_files = FileSensor(
        task_id="check_files",
        filepath="/opt/airflow/data/vms/incoming/vms_sample.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    extract_data_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    validate_schema_task = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
    )

    upload_to_adl_task = PythonOperator(
        task_id="upload_to_adl",
        python_callable=upload_to_adl,
    )

    archive_files = BashOperator(
        task_id="archive_files",
        bash_command="""
        mkdir -p /opt/airflow/data/vms/processed && \
        mv /opt/airflow/data/vms/incoming/*.csv /opt/airflow/data/vms/processed/ || true && \
        rm -f /opt/airflow/data/vms/staging/*_clean.csv || true && \
        rm -f /opt/airflow/data/vms/staging/*_validated.csv || true
        """,
    )

    check_files >> extract_data_task >> validate_schema_task >> upload_to_adl_task >> archive_files

