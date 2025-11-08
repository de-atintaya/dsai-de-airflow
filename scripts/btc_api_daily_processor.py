import polars as pl
import os
import logging
from scripts.azure_upload import upload_delta_lake_to_adls

# -- Logging Configuration --
log = logging.getLogger("airflow.task")

# -- Configuration --
# Directory where your Parquet files are stored.
INPUT_DIR = "data"
# Path for the output Delta Lake table.
OUTPUT_DELTA_TABLE = "data/btc_daily_stats.delta"
CONTAINER_NAME = "airflow"
REMOTE_DELTA_TABLE_NAME = "raw/airflow/g4/btc_daily_stats"
WASB_CONN_ID = "azure_blob_storage"


def analyze_data(
    INPUT_DIR: str = INPUT_DIR,
    OUTPUT_DELTA_TABLE: str = OUTPUT_DELTA_TABLE,
    CONTAINER_NAME: str = CONTAINER_NAME,
    REMOTE_DELTA_TABLE_NAME: str = REMOTE_DELTA_TABLE_NAME,
    WASB_CONN_ID: str = WASB_CONN_ID,
):
    """
    Reads all Parquet files from the input directory, calculates daily
    statistics, and writes the result to a Delta Lake table.
    """
    parquet_files_path = os.path.join(INPUT_DIR, "*.parquet")
    log.info(f"Searching for Parquet files in: {parquet_files_path}")

    try:
        # Lazily scan all parquet files to handle large datasets
        lazy_df = pl.scan_parquet(parquet_files_path)
    except Exception as e:
        log.error(f"An error occurred while reading Parquet files: {e}")
        log.warning("Please ensure there are Parquet files in the specified directory.")
        return

    log.info("Calculating daily statistics...")

    # Group by day and calculate descriptive statistics
    daily_stats_df = (
        lazy_df.group_by(pl.col("timestamp").dt.date().alias("date"))
        .agg(
            pl.col("price").min().alias("min_price"),
            pl.col("price").max().alias("max_price"),
            pl.col("price").mean().alias("mean_price"),
            pl.col("price").median().alias("median_price"),
            pl.col("price").std().alias("std_dev_price"),
            pl.col("price").var().alias("variance_price"),
            pl.col("price").quantile(0.25).alias("q1_price"),
            pl.col("price").quantile(0.75).alias("q3_price"),
            pl.col("price").first().alias("open_price"),
            pl.col("price").last().alias("close_price"),
            pl.count().alias("record_count"),
        )
        .sort("date")
    )

    # Calculate Interquartile Range (IQR)
    daily_stats_df = daily_stats_df.with_columns(
        (pl.col("q3_price") - pl.col("q1_price")).alias("iqr_price")
    )

    # Collect the results from the lazy evaluation
    final_df = daily_stats_df.collect()

    if final_df.is_empty():
        log.warning("No data found to process. Exiting.")
        return

    log.info("Daily statistics calculated:")
    log.info(f"\n{final_df}")

    try:
        log.info(f"Writing data to Delta Lake table: {OUTPUT_DELTA_TABLE}")
        final_df.write_delta(OUTPUT_DELTA_TABLE, mode="overwrite")
        log.info("Successfully wrote to Delta Lake table.")

        # Upload the Delta Lake table to Azure
        upload_delta_lake_to_adls(
            local_delta_table_path=OUTPUT_DELTA_TABLE,
            container_name=CONTAINER_NAME,
            remote_delta_table_name=REMOTE_DELTA_TABLE_NAME,
            wasb_conn_id=WASB_CONN_ID,
        )
    except Exception as e:
        log.error(
            f"An error occurred while writing to the Delta Lake table or uploading to Azure: {e}"
        )


if __name__ == "__main__":
    analyze_data(
        INPUT_DIR=INPUT_DIR,
        OUTPUT_DELTA_TABLE=OUTPUT_DELTA_TABLE,
        CONTAINER_NAME=CONTAINER_NAME,
        REMOTE_DELTA_TABLE_NAME=REMOTE_DELTA_TABLE_NAME,
        WASB_CONN_ID=WASB_CONN_ID,
    )
