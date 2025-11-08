import polars as pl

a = pl.scan_parquet("data/*.parquet")
b = pl.scan_delta("data/btc_daily_stats.delta")

if __name__ == "__main__":
    print(a.collect())
    print(b.collect())
