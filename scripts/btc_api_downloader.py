import asyncio
import aiohttp
import polars as pl
from datetime import datetime, timedelta
import logging
import os
from scripts.azure_upload import upload_to_adls

# -- Logging Configuration --
log = logging.getLogger("airflow.task")


async def collect_data(
    OUTPUT_FILE: str,
    BLOB_NAME: str,
    CONTAINER_NAME: str,
    WASB_CONN_ID: str,
    API_URL: str = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
    COLLECTION_INTERVAL_SECONDS: int = 10,
    TOTAL_DURATION_MINUTES: int = 1,
):
    """
    Collects data from the Binance API asynchronously for a specified duration
    and saves it to a Parquet file using polars.
    """
    log.info(f"Starting data collection for {TOTAL_DURATION_MINUTES} minute(s).")
    log.info(f"Data will be fetched every {COLLECTION_INTERVAL_SECONDS} second(s).")

    collected_data = []
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=TOTAL_DURATION_MINUTES)

    async with aiohttp.ClientSession() as session:
        while datetime.now() < end_time:
            loop_start_time = asyncio.get_event_loop().time()

            try:
                async with session.get(API_URL) as response:
                    response.raise_for_status()  # Raise an exception for bad status codes
                    data = await response.json()

                    # Add a timestamp to the collected data
                    data["timestamp"] = datetime.now()
                    collected_data.append(data)

                    log.info(
                        f"Collected: {data['symbol']} - {data['price']} at {data['timestamp'].isoformat()}"
                    )

            except aiohttp.ClientError as e:
                log.error(f"An error occurred while requesting data: {e}")
            except Exception as e:
                log.error(f"An unexpected error occurred: {e}")

            # Wait for the next interval
            elapsed_time = asyncio.get_event_loop().time() - loop_start_time
            sleep_time = max(0, COLLECTION_INTERVAL_SECONDS - elapsed_time)
            await asyncio.sleep(sleep_time)

    if not collected_data:
        log.warning("No data was collected. Exiting.")
        return

    # Convert to polars DataFrame and save as Parquet
    df = pl.DataFrame(collected_data)
    # Cast columns to appropriate types for better storage
    df = df.with_columns(
        pl.col("timestamp").cast(pl.Datetime), pl.col("price").cast(pl.Float64)
    )

    try:
        # Ensure the output directory exists
        output_dir = os.path.dirname(OUTPUT_FILE)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        df.write_parquet(OUTPUT_FILE)
        log.info(
            f"Data collection finished. Saved {len(df)} records to '{OUTPUT_FILE}'."
        )

        # Upload the file to Azure
        upload_to_adls(
            local_file_path=OUTPUT_FILE,
            container_name=CONTAINER_NAME,
            blob_name=BLOB_NAME,
            wasb_conn_id=WASB_CONN_ID,
        )
    except Exception as e:
        log.error(
            f"An error occurred while saving the Parquet file or uploading to Azure: {e}"
        )


if __name__ == "__main__":
    # -- Configuration for local run --
    output_dir = "data"
    now_iso = datetime.now().isoformat()
    output_file = f"{output_dir}/btc_prices_{now_iso}.parquet"
    blob_name = f"raw/airflow/g4/btc_prices/{os.path.basename(output_file)}"
    container_name = "airflow"
    wasb_conn_id = "azure_blob_storage"

    asyncio.run(
        collect_data(
            OUTPUT_FILE=output_file,
            BLOB_NAME=blob_name,
            CONTAINER_NAME=container_name,
            WASB_CONN_ID=wasb_conn_id,
        )
    )
