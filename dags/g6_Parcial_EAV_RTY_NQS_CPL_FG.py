from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta

LOCAL_FILE_PATHS = ["/opt/airflow/data/2023_01_producao_Mar.csv",
                    "/opt/airflow/data/2023_01_producao_Presal.csv",
                    "/opt/airflow/data/2023_01_producao_Terra.csv",
                    "/opt/airflow/data/2023_02_producao_Mar.csv",
                    "/opt/airflow/data/2023_02_producao_Presal.csv",
                    "/opt/airflow/data/2023_02_producao_Terra.csv",
                    "/opt/airflow/data/2023_03_producao_Mar.csv",
                    "/opt/airflow/data/2023_03_producao_Presal.csv",
                    "/opt/airflow/data/2023_03_producao_Terra.csv",
                    "/opt/airflow/data/2023_04_producao_Mar.csv",
                    "/opt/airflow/data/2023_04_producao_Presal.csv",
                    "/opt/airflow/data/2023_04_producao_Terra.csv",
                    "/opt/airflow/data/2023_05_producao_Mar.csv",
                    "/opt/airflow/data/2023_05_producao_Presal.csv",
                    "/opt/airflow/data/2023_05_producao_Terra.csv",
                    "/opt/airflow/data/2023_06_producao_Mar.csv",
                    "/opt/airflow/data/2023_06_producao_Presal.csv",
                    "/opt/airflow/data/2023_06_producao_Terra.csv",
                    ]
CONTAINER_NAME = "airflow"

default_args = {
    'owner': 'grupo6',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

@dag(
    dag_id="g6_Parcial_EAV_RTY_NQS_CPL_FG",
    description="Pipeline de ingesta diaria de data de petróleo crudo.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 8 * * *", # Runs every monday at 12:00 local time (GMT-5)
    catchup=False,
    tags=["grupo6", "campo_produccion", "petroleo_crudo", "azure", "blob", "upload" ],
)
def upload_dag():

    @task
    def call_upload():
        for file_path in LOCAL_FILE_PATHS:
            file_name=file_path.split("/")[-1]
            new_blob_name=add_date_suffix(f"raw/G6/{file_name}")
            upload_to_adls(
                local_file_path=file_path,
                container_name=CONTAINER_NAME,
                blob_name=new_blob_name
            )



    call_upload()

dag = upload_dag()
