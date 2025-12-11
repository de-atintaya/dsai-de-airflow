# Databricks notebook source
my_catalog = 'g4_catalog'
my_prefix = 'MBRSA'

# COMMAND ----------

table_bronze_binance_prices = f'{my_catalog}.bronze.{my_prefix}_binance_prices'
table_bronze_portfolio_snapshots = f'{my_catalog}.bronze.{my_prefix}_portfolio_snapshots'

# COMMAND ----------

spark.sql(f"""DROP TABLE IF EXISTS {table_bronze_binance_prices}""")
spark.sql(f"""DROP TABLE IF EXISTS {table_bronze_portfolio_snapshots}""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {table_bronze_binance_prices} AS
    SELECT *, current_timestamp() AS inserted_at
    FROM read_files(
        'abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/g4/crypto_prices/*.parquet',
        format => 'parquet',
        schema => 'symbol STRING, price DOUBLE, timestamp TIMESTAMP' 
    )    
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {table_bronze_portfolio_snapshots} AS
    SELECT *, current_timestamp() AS inserted_at
    FROM read_files(
        'abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/g4/holdings_portfolio_snapshots/*.parquet',
        format => 'parquet',
        schema => 'snapshot_timestamp TIMESTAMP,	account_id STRING,	asset_symbol STRING,	quantity DOUBLE,	acquisition_cost DOUBLE' 
    )    
""")

# COMMAND ----------

spark.sql(f"""
          select * from {table_bronze_binance_prices}
          """).display()

# COMMAND ----------

spark.sql(f"""
          select * from {table_bronze_portfolio_snapshots}
          """).display()

# COMMAND ----------

spark.sql(
    f"""
    SELECT
        CASE WHEN COUNT(*) > 0 THEN raise_error('price is null')
            ELSE 'Datos validados: No hay precios nulos in {table_bronze_binance_prices}'
        END AS resultados
    FROM {table_bronze_binance_prices}
    WHERE price IS NULL
    """
).display()

# COMMAND ----------

# spark.sql(f"""SELECT * FROM {table_bronze}""").display()