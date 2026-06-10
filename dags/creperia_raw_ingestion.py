from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from datetime import datetime, timedelta
import os
import logging

LOCAL_INPUT_PATH = "/opt/airflow/data/input"

CONTAINER_NAME = "datalake"
WASB_CONN_ID = "utec_blob_storage"

GROUP_NAME = "G3"

FILES = [
    "customers.csv",
    "products.csv",
    "suppliers.csv",
    "inventory.csv",
    "orders.csv",
    "order_items.csv"
]

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="creperia_raw_ingestion",
    description="Ingesta de archivos CSV de una creperia hacia Raw en el Data Lake de UTEC.",
    default_args=default_args,
    start_date=datetime(2026, 6, 1, tzinfo=timezone("America/Lima")),
    schedule=None,
    catchup=False,
    tags=["creperia", "utec", "azure", "raw", "airflow2"],
)
def creperia_raw_ingestion():

    @task
    def validate_files():
        logging.info("Starting file validation process...")
        logging.info(f"Input folder: {LOCAL_INPUT_PATH}")
        logging.info(f"Expected files: {FILES}")

        for file_name in FILES:
            file_path = os.path.join(LOCAL_INPUT_PATH, file_name)
            logging.info(f"Validating file: {file_name}")

            if not os.path.exists(file_path):
                logging.error(f"File not found: {file_path}")
                raise FileNotFoundError(f"File not found: {file_path}")

            if not file_name.endswith(".csv"):
                logging.error(f"Invalid file format: {file_name}")
                raise ValueError(f"Invalid file format: {file_name}")

            if os.path.getsize(file_path) == 0:
                logging.error(f"Empty file: {file_name}")
                raise ValueError(f"Empty file: {file_name}")

            file_size = os.path.getsize(file_path)
            logging.info(f"Validated file successfully: {file_name} | Size: {file_size} bytes")

        logging.info(f"File validation completed successfully. {len(FILES)} files validated.")

    @task
    def upload_files_to_raw():
        logging.info("Starting upload process to UTEC Data Lake...")
        logging.info(f"Azure container: {CONTAINER_NAME}")
        logging.info(f"Airflow connection id: {WASB_CONN_ID}")
        logging.info(f"Group folder: {GROUP_NAME}")

        load_date = datetime.today().strftime("%Y-%m-%d")
        uploaded_files = 0

        for file_name in FILES:
            table_name = file_name.replace(".csv", "")
            local_file_path = os.path.join(LOCAL_INPUT_PATH, file_name)

            blob_name = f"raw/airflow2/{GROUP_NAME}/creperia/{table_name}/load_date={load_date}/{file_name}"

            logging.info(f"Uploading file: {file_name}")
            logging.info(f"Source path: {local_file_path}")
            logging.info(f"Destination path: {CONTAINER_NAME}/{blob_name}")

            upload_to_adls(
                local_file_path=local_file_path,
                container_name=CONTAINER_NAME,
                blob_name=blob_name,
                wasb_conn_id=WASB_CONN_ID
            )

            uploaded_files += 1
            logging.info(f"Uploaded successfully: {file_name}")

        logging.info(f"Upload process completed successfully. {uploaded_files} files uploaded to Raw Zone.")
        logging.info(f"Final Raw path: {CONTAINER_NAME}/raw/airflow2/{GROUP_NAME}/creperia/")

    validate_files() >> upload_files_to_raw()

dag = creperia_raw_ingestion()