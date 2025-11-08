from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime, timedelta
from scripts.btc_api_daily_processor import analyze_data

# -- Configuration --
INPUT_DIR = "data"
OUTPUT_DELTA_TABLE = "data/btc_daily_stats.delta"
CONTAINER_NAME = "datalake"
REMOTE_DELTA_TABLE_NAME = "raw/airflow/g4/btc_daily_stats"
WASB_CONN_ID = "proyecto_azure_blob_storage"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="g4_MBRSA_binance_daily_stats",
    description="Runs the Binance BTC daily stats processor.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 0 * * *",  # Runs every day at midnight
    catchup=False,
    tags=["utec", "binance", "processor", "daily"],
)
def binance_daily_stats_dag():
    @task
    def run_daily_processor():
        analyze_data(
            INPUT_DIR=INPUT_DIR,
            OUTPUT_DELTA_TABLE=OUTPUT_DELTA_TABLE,
            CONTAINER_NAME=CONTAINER_NAME,
            REMOTE_DELTA_TABLE_NAME=REMOTE_DELTA_TABLE_NAME,
            WASB_CONN_ID=WASB_CONN_ID,
        )

    run_daily_processor()


dag = binance_daily_stats_dag()
