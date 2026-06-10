from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_folder_to_adls 
from datetime import datetime, timedelta
import os

BASE_LOCAL_PATH = "/opt/airflow/data"
STAGING_LOCAL_PATH = "/opt/airflow/data/staging"
CONTAINER_NAME = "airflow"
BLOB_PREFIX = "raw/G1"

FOLDERS_TO_UPLOAD = [
    "sensor_archivo_notas",
    "sensor_archivo_asistencia"
]
default_args = {
    'owner': 'G1',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g1_parcial_JDYG",
    description="Uploads multiple local folders to Azure Blob Storage dynamically.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 6 * * *", # Runs every day at 6:00 AM
    catchup=False,
    tags=["G1", "asistencia", "notas","azure", "blob", "upload"],
)
def upload_dag():

    @task
    def transform_data(folder_name: str) -> str:
        origin_path = os.path.join(BASE_LOCAL_PATH, folder_name)
        target_path = os.path.join(STAGING_LOCAL_PATH, folder_name)
        
        os.makedirs(target_path, exist_ok=True)
        
        if not os.path.exists(origin_path):
            raise FileNotFoundError(f"No se encontró la carpeta origen: {origin_path}")

        for root, dirs, files in os.walk(origin_path):
            for file in files:
                file_origin = os.path.join(root, file)
                file_target = os.path.join(target_path, file)
                
                with open(file_origin, 'r', encoding='utf-8', errors='ignore') as f_in:
                    lines = f_in.readlines()
                
                cleaned_lines = [line.strip() for line in lines if line.strip()]
                
                with open(file_target, 'w', encoding='utf-8') as f_out:
                    f_out.write("\n".join(cleaned_lines) + "\n")
        return target_path

    @task
    def call_upload(transformed_folder_path: str, folder_name: str):
        azure_dest_prefix = f"{BLOB_PREFIX}/{folder_name}"

        upload_folder_to_adls(
            local_folder_path=transformed_folder_path,
            container_name=CONTAINER_NAME,
            blob_prefix=azure_dest_prefix
        )

    transformed_paths = transform_data.expand(folder_name=FOLDERS_TO_UPLOAD)

    call_upload.expand_kwargs(
        transformed_paths.map(lambda path: {
            "transformed_folder_path": path, 
            "folder_name": os.path.basename(path)
        })
    )

dag = upload_dag()