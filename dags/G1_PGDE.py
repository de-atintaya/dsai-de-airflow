# %%

from airflow.sdk import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta
from pathlib import Path

# %%

Data_DIR = Path("/opt/airflow/data/g1_data")
CONTAINER_NAME = "airflow"
# BLOB_NAME = "raw/G1/archivo_subido.txt"


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g1_pgde",
    description="Uploads a local file to Azure Blob Storage with a date suffix.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 12 * * 1", # Runs every monday at 12:00 local time (GMT-5)
    catchup=False,
    tags=["azure", "blob", "upload"],
)
def upload_dag():

    @task
    def build_file_list() -> list[dict[str, str]]:
        files: list = []
        for file in Data_DIR.glob("*.txt"):
            files.append(
                {
                    "local_file_path": str(file),
                    "blob_name": f"raw/G1/{file.name}"
                }
            )
        return files


    @task
    def call_upload(file_cfg: dict[str, str]):
        upload_to_adls(
            local_file_path=file_cfg["local_file_path"],
            container_name=CONTAINER_NAME,
            blob_name=add_date_suffix(file_cfg["blob_name"])
            )

    call_upload.expand(file_cfg=build_file_list())

dag = upload_dag()
