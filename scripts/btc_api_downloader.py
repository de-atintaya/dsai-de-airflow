import asyncio
import aiohttp
import polars as pl
from datetime import datetime, timedelta
import logging
import os
from airflow.exceptions import AirflowFailException

# -- Logging Configuration --
log = logging.getLogger("airflow.task")


async def fetch_symbol_data(session, base_url, symbol):
    url = f"{base_url}?symbol={symbol}"
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.json()


async def collect_and_save_data(
    OUTPUT_FILE: str,
    SYMBOLS: list[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    BASE_URL: str = "https://api.binance.com/api/v3/ticker/price",
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
                tasks = [
                    fetch_symbol_data(session, BASE_URL, symbol) for symbol in SYMBOLS
                ]
                results = await asyncio.gather(*tasks)

                timestamp = datetime.now()
                for data in results:
                    # Add a timestamp to the collected data
                    data["timestamp"] = timestamp
                    collected_data.append(data)

                    log.info(
                        f"Collected: {data['symbol']} - {data['price']} at {data['timestamp'].isoformat()}"
                    )

            except aiohttp.ClientError as e:
                log.error(f"An error occurred while requesting data: {e}")
                raise AirflowFailException(f"API request failed: {e}")
            except Exception as e:
                log.error(f"An unexpected error occurred: {e}")
                raise

            # Wait for the next interval
            elapsed_time = asyncio.get_event_loop().time() - loop_start_time
            sleep_time = max(0, COLLECTION_INTERVAL_SECONDS - elapsed_time)
            await asyncio.sleep(sleep_time)

    if not collected_data:
        log.warning("No data was collected. Exiting.")
        raise AirflowFailException("No data was collected from the API.")

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
    except Exception as e:
        log.error(f"An error occurred while saving the Parquet file: {e}")
        raise AirflowFailException(f"Failed to save Parquet file: {e}")


if __name__ == "__main__":
    # -- Configuration for local run --
    output_dir = "data"
    now_iso = datetime.now().isoformat()
    output_file = f"{output_dir}/crypto_prices_{now_iso}.parquet"

    asyncio.run(
        collect_and_save_data(
            OUTPUT_FILE=output_file,
        )
    )
