# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

my_catalog = 'g10_rie_alertas_normas'

normas_silver = 'mv_normas_parseadas'
sectores_silver = 'mv_sectores'
table_transform_normas = f'{my_catalog}.silver.{normas_silver}'
table_transform_sectores = f'{my_catalog}.silver.{sectores_silver}'


# COMMAND ----------

normas_gold = 'vw_alertas_normas'
table_gold_normas = f'{my_catalog}.gold.{normas_gold}'

# COMMAND ----------

# Leer desde Silver
df_normas   = spark.table(table_transform_normas)
df_sectores = spark.table(table_transform_sectores)

# COMMAND ----------

# Explode de palabras clave de sectores
df_sect_exp = (
    df_sectores
    .select(
        "sector_id",
        "sector",
        F.explode("palabras_array").alias("keyword_raw")
    )
    .withColumn("keyword", F.trim(F.col("keyword_raw")))
    .filter(F.col("keyword") != "")  # quitar vacíos
)

# COMMAND ----------

# Match por keyword contenida en el texto de la norma
df_alertas = (
    df_normas.crossJoin(df_sect_exp)
    .where(
        df_normas.texto_busqueda.contains(df_sect_exp.keyword)
    )
    .select(
        df_normas.id_norma,
        df_normas.titulo,
        df_normas.fecha_publicacion,
        df_normas.entidad,
        df_normas.url,
        df_sect_exp.sector_id,
        df_sect_exp.sector,
        df_sect_exp.keyword.alias("keyword_match"),
        df_normas.fecha_proceso
    )
)

# COMMAND ----------

df_alertas.show()

# COMMAND ----------

df_alertas.write.mode("overwrite").format("delta").saveAsTable(table_gold_normas)

# COMMAND ----------

