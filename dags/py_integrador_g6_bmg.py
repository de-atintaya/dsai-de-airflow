from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime, timedelta
import os
from scripts.azure_upload import upload_to_adls
from scripts.helpers import leer_datos, transformar_datos, guardar_datos, add_date_suffix

LOCAL_DATA_DIR = "/opt/airflow/data"
CONTAINER_NAME = "datalake" # airflow
WASB_CONN_ID = "utec_blob_storage"
BLOB_PREFIX = "raw/G6"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="py_integrador_g6_bmg",
    description="Ingest local files from data/ into Azure Blob Storage (raw).",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 23 * * *",  # daily at 23:00
    catchup=False,
    tags=["azure", "ingest", "raw"],
)
def ingest_dag():

    @task
    def list_input_files():
        if not os.path.exists(LOCAL_DATA_DIR):
            return []
        files = [os.path.join(LOCAL_DATA_DIR, f) for f in os.listdir(LOCAL_DATA_DIR) if os.path.isfile(os.path.join(LOCAL_DATA_DIR, f))]
        return files

    @task
    def process_files(file_paths: list):
        processed = []
        for path in file_paths:
            try:
                filename = os.path.basename(path)
                name, ext = os.path.splitext(filename)
                if ext.lower() in ['.csv']:
                    # minimal transformation pipeline
                    df = leer_datos(path)
                    df_t = transformar_datos(df)
                    out_name = f"{name}_transformed.csv"
                    out_path = os.path.join(LOCAL_DATA_DIR, out_name)
                    guardar_datos(df_t, out_path)
                    processed.append(out_path)
                else:
                    # pass-through for other files
                    processed.append(path)
            except Exception as e:
                # continue with next file (individual file failure shouldn't stop the whole DAG)
                from airflow.utils.log.logging_mixin import LoggingMixin
                log = LoggingMixin().log
                log.error(f"Failed processing {path}: {e}")
        return processed

    @task
    def upload_processed(files: list):
        results = []
        for p in files:
            blob_name = f"{BLOB_PREFIX}/{add_date_suffix(os.path.basename(p))}"
            try:
                upload_to_adls(local_file_path=p, container_name=CONTAINER_NAME, blob_name=blob_name)
                results.append({'file': p, 'status': 'uploaded', 'blob': blob_name})
            except Exception as e:
                results.append({'file': p, 'status': 'failed', 'error': str(e)})
        return results

    files = list_input_files()
    processed = process_files(files)
    upload_results = upload_processed(processed)

    return upload_results


dag = ingest_dag()
