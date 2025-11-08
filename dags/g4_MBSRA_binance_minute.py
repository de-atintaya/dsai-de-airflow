from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime, timedelta
import asyncio
from scripts.btc_api_downloader import collect_data
import os

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="g4_MBRSA_binance_minute_downloader",
    description="Runs the Binance BTC price downloader every minute.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="* * * * *",  # Runs every minute
    catchup=False,
    tags=["utec", "binance", "downloader"],
)
def binance_downloader_dag():
    @task
    def run_downloader():
        # -- Configuration --
        # Generate dynamic file paths inside the task to ensure uniqueness for each run
        now_iso = datetime.now().isoformat()
        output_dir = "data"
        output_file = f"{output_dir}/btc_prices_{now_iso}.parquet"
        blob_name = f"raw/airflow/g4/btc_prices/{os.path.basename(output_file)}"
        container_name = "datalake"
        wasb_conn_id = "proyecto_azure_blob_storage"

        # Default script parameters
        api_url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
        collection_interval_seconds = 10
        total_duration_minutes = 1

        asyncio.run(
            collect_data(
                OUTPUT_FILE=output_file,
                BLOB_NAME=blob_name,
                CONTAINER_NAME=container_name,
                WASB_CONN_ID=wasb_conn_id,
                API_URL=api_url,
                COLLECTION_INTERVAL_SECONDS=collection_interval_seconds,
                TOTAL_DURATION_MINUTES=total_duration_minutes,
            )
        )

    run_downloader()


dag = binance_downloader_dag()
