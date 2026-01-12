from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime, timedelta
from scripts.btc_api_daily_processor import process_and_save_daily_stats
from scripts.azure_upload import upload_delta_lake_to_adls

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
    description="Processes Binance BTC data and uploads daily stats to Azure.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 */12 * * *",
    catchup=False,
    tags=["utec", "binance", "processor", "daily"],
)
def binance_daily_stats_dag():
    @task
    def process_data():
        """
        Processes local parquet files to generate daily statistics and saves them
        to a local Delta Lake table.
        """
        process_and_save_daily_stats(
            INPUT_DIR=INPUT_DIR,
            OUTPUT_DELTA_TABLE=OUTPUT_DELTA_TABLE,
        )

    @task
    def upload_to_azure():
        """
        Uploads the local Delta Lake table to Azure Data Lake Storage.
        """
        upload_delta_lake_to_adls(
            local_delta_table_path=OUTPUT_DELTA_TABLE,
            container_name=CONTAINER_NAME,
            remote_delta_table_name=REMOTE_DELTA_TABLE_NAME,
            wasb_conn_id=WASB_CONN_ID,
        )

    # Set task dependency
    process_data() >> upload_to_azure()


dag = binance_daily_stats_dag()
