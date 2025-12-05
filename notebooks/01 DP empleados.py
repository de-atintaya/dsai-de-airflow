# Databricks notebook source
from pyspark.sql.functions import current_date, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType

# COMMAND ----------

my_catalog = 'g10_tal_colaboradores'
my_prefix = 'empleados'

# COMMAND ----------

table_input = f'{my_catalog}.bronze.{my_prefix}'

# COMMAND ----------

source_path = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G10/final/empleados.csv"

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {table_input}')

# COMMAND ----------

df_empleados = (
    spark.read
    .option("header", "true")        # primera fila como nombres
    .option("inferSchema", "true")   # inferir tipos
    .option("delimiter", ",")        # separador por punto y coma
    .csv(source_path)
)

# COMMAND ----------

df_empleados.show()

# COMMAND ----------

df_empleados = df_empleados.withColumn("inserted_at", current_timestamp())

# COMMAND ----------

df_empleados.write.format("delta").mode("overwrite").saveAsTable(table_input)

# COMMAND ----------

