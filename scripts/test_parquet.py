import polars as pl

# a = pl.scan_parquet("data/*.parquet")
b = pl.scan_delta("data/btc_daily_stats.delta")
d = pl.scan_csv("data/holdings_portfolio_snapshots.csv")
c = pl.scan_parquet("data/holdings_portfolio_snapshots.parquet")

if __name__ == "__main__":
    #   print(a.collect())
    print(b.collect())
    print(c.collect())
    print(d.collect())
