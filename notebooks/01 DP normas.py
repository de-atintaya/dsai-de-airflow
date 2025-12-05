# Databricks notebook source
from pyspark.sql.functions import current_date, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType

# COMMAND ----------

my_catalog = 'g10_rie_alertas_normas'
normas_csv = 'normas'
normas_json = 'normas_contenido'
sectores_csv = 'sectores'

# COMMAND ----------

table_input_normas_csv = f'{my_catalog}.bronze.{normas_csv}'
table_input_normas_json = f'{my_catalog}.bronze.{normas_json}'
table_input_sectores_csv = f'{my_catalog}.bronze.{sectores_csv}'

# COMMAND ----------

source_path_normas_csv = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G10/final/normas_peruano.csv"
source_path_normas_json = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G10/final/normas_html.json"
source_path_sectores_csv = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G10/final/cat_sectores.csv"

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {table_input_normas_csv}')
spark.sql(f'DROP TABLE IF EXISTS {table_input_normas_json}')
spark.sql(f'DROP TABLE IF EXISTS {table_input_sectores_csv}')

# COMMAND ----------

df_normas = (
    spark.read
    .option("header", "true")        # primera fila como nombres
    .option("inferSchema", "true")   # inferir tipos
    .option("delimiter", ",")        # separador por punto y coma
    .csv(source_path_normas_csv)
)

# COMMAND ----------

df_sectores = (
    spark.read
    .option("header", "true")        # primera fila como nombres
    .option("inferSchema", "true")   # inferir tipos
    .option("delimiter", ",")        # separador por punto y coma
    .csv(source_path_sectores_csv)
)

# COMMAND ----------

df_normas_html = (
    spark.read
         .option("multiline", "true")  # por si el JSON es un array
         .json(source_path_normas_json)
)

# COMMAND ----------

df_normas = df_normas.withColumn("inserted_at", current_timestamp())
df_sectores = df_sectores.withColumn("inserted_at", current_timestamp())
df_normas_html = df_normas_html.withColumn("inserted_at", current_timestamp())

# COMMAND ----------

df_sectores.show()

# COMMAND ----------

df_normas.write.format("delta").mode("overwrite").saveAsTable(table_input_normas_csv)
df_normas_html.write.format("delta").mode("overwrite").saveAsTable(table_input_normas_json)
df_sectores.write.format("delta").mode("overwrite").saveAsTable(table_input_sectores_csv)

# COMMAND ----------



# COMMAND ----------

