import polars as pl
from datetime import datetime, timedelta
import logging
from airflow.exceptions import AirflowFailException
from pathlib import Path

# -- Logging Configuration --
log = logging.getLogger("airflow.task")

def process_portfolio_holdings(portfolio_holdings_file_path: Path):
    try:
        # Read the portfolio holdings file
        portfolio_holdings = pl.read_csv(portfolio_holdings_file_path,separator="," ).with_columns(date_uploaded=pl.lit(datetime.now()))
        log.info(f"Read holdings file: {portfolio_holdings_file_path.name}, last available info: {portfolio_holdings.select(pl.col("snapshot_timestamp")).max().item(0,0)}")
        portfolio_holdings.write_parquet(portfolio_holdings_file_path.with_suffix(".parquet"))
        log.info(f"Wrote holdings file: {portfolio_holdings_file_path.with_suffix(".parquet")}")
    except Exception as e:
        log.error(f"An error occurred while processing the portfolio holdings file: {e}")
        raise AirflowFailException(f"Failed to process portfolio holdings file: {e}")
    return portfolio_holdings_file_path.with_suffix(".parquet")

if __name__ == "__main__":
    # -- Configuration for local run --
    portfolio_holdings_file_path = Path("data/holdings_portfolio_snapshots.csv")
    process_portfolio_holdings(portfolio_holdings_file_path)



