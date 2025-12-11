import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from pendulum import timezone

from scripts.azure_upload import upload_to_adls
from scripts.portfolio_holding_daily_processor import process_portfolio_holdings

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="g4_MBRSA_portfolio_processor_uploader",
    description="Processes portfolio holdings and uploads it to Azure.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 0 * * *",  # Runs every day
    catchup=False,
    tags=["utec", "portfolio", "uploader"],
)
def portfolio_processor_dag():
    @task
    def process_holdings():
        """
        Processes portfolio holdings from a .csv file and returns the file path and blob name for the next task.
        """
        # -- Configuration --
        # Generate dynamic file paths inside the task to ensure uniqueness for each run
        input_file = Path(f"data/holdings_portfolio_snapshots.csv")

        # Default script parameters
        output_file = process_portfolio_holdings(input_file)
        blob_name = f"raw/airflow/g4/holdings_portfolio_snapshots/{os.path.basename(output_file)}"
        return {"output_file": output_file.as_posix(), "blob_name": blob_name}

    @task
    def upload_to_azure(paths: dict):
        """
        Uploads the local Parquet file to Azure Data Lake Storage.
        """
        container_name = "datalake"
        wasb_conn_id = "proyecto_azure_blob_storage"

        upload_to_adls(
            local_file_path=paths["output_file"],
            container_name=container_name,
            blob_name=paths["blob_name"],
            wasb_conn_id=wasb_conn_id,
        )

    # Set task dependency
    file_paths = process_holdings()
    upload_to_azure(file_paths)


dag = portfolio_processor_dag()
