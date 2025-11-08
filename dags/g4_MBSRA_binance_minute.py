from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime, timedelta
import asyncio
import os
from scripts.btc_api_downloader import collect_and_save_data
from scripts.azure_upload import upload_to_adls

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="g4_MBRSA_binance_minute_downloader",
    description="Downloads Binance BTC price data and uploads it to Azure.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="* * * * *",  # Runs every minute
    catchup=False,
    tags=["utec", "binance", "downloader"],
)
def binance_downloader_dag():
    @task
    def download_data():
        """
        Downloads BTC price data from Binance API and saves it to a local Parquet file.
        Returns the file path and blob name for the next task.
        """
        # -- Configuration --
        # Generate dynamic file paths inside the task to ensure uniqueness for each run
        now_iso = datetime.now().isoformat()
        output_dir = "data"
        output_file = f"{output_dir}/btc_prices_{now_iso}.parquet"
        blob_name = f"raw/airflow/g4/btc_prices/{os.path.basename(output_file)}"

        # Default script parameters
        api_url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
        collection_interval_seconds = 10
        total_duration_minutes = 1

        asyncio.run(
            collect_and_save_data(
                OUTPUT_FILE=output_file,
                API_URL=api_url,
                COLLECTION_INTERVAL_SECONDS=collection_interval_seconds,
                TOTAL_DURATION_MINUTES=total_duration_minutes,
            )
        )

        return {"output_file": output_file, "blob_name": blob_name}

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
    file_paths = download_data()
    upload_to_azure(file_paths)


dag = binance_downloader_dag()
