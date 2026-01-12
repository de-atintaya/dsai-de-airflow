# Databricks notebook source
my_catalog = 'g4_catalog'
my_prefix = 'MBRSA'

# COMMAND ----------

schema = 'silver' 

table_bronze_prices = f'{my_catalog}.bronze.{my_prefix}_binance_prices'
table_bronze_portfolio = f'{my_catalog}.bronze.{my_prefix}_portfolio_snapshots'

table_silver_prices = f'{my_catalog}.silver.{my_prefix}_market_prices_clean'
table_silver_holdings = f'{my_catalog}.silver.{my_prefix}_portfolio_positions'

# COMMAND ----------

spark.sql(f"""DROP TABLE IF EXISTS {table_silver_prices}""")

spark.sql(f"""
    CREATE TABLE {table_silver_prices} AS
    SELECT
        timestamp as market_time,
        symbol as raw_symbol,
        -- TRANSFORMACION: Removemos 'USDT' para coincidir con el simbolo del portafolio
        replace(symbol, 'USDT', '') as asset_symbol, 
        price,
        -- Flaggeamos si el precio es negativo o cero
        CASE WHEN price <= 0 THEN 'INVALID' ELSE 'VALID' END as quality_flag
    FROM {table_bronze_prices}
    WHERE price IS NOT NULL
""")

# COMMAND ----------

spark.sql(f"""DROP TABLE IF EXISTS {table_silver_holdings}""")

spark.sql(f"""
    CREATE TABLE {table_silver_holdings} AS
    SELECT 
        snapshot_timestamp,
        account_id,
        asset_symbol,
        cast(quantity as double) as quantity,
        cast(acquisition_cost as double) as acquisition_cost
    FROM {table_bronze_portfolio}
""")

# COMMAND ----------

print("Silver Layer Creada.")
spark.sql(f"SELECT * FROM {table_silver_prices} LIMIT 5").display()

# COMMAND ----------

spark.sql(f"SELECT * FROM {table_silver_holdings} LIMIT 5").display()