# Databricks notebook source
from pyspark.sql.functions import col, count

# COMMAND ----------

my_catalog = 'g10_tal_colaboradores'
my_prefix = 'vw_empleados'

# COMMAND ----------

table_transform = f'{my_catalog}.silver.mv_empleados_normalizado'
table_summary = f'{my_catalog}.gold.{my_prefix}'

# COMMAND ----------

# Asegurarnos de leer la tabla Silver
df_silver_normalizado = spark.table(table_transform)

# Gold: vista de negocio
df_gold = df_silver_normalizado.select(
    "empleado_id",
    "nombre_completo",
    "area"
)

# COMMAND ----------

df_gold.show(2)

# COMMAND ----------

# Opcional: guardar como tabla física
df_gold.write.mode("overwrite").format("delta").saveAsTable(table_summary)

# COMMAND ----------

