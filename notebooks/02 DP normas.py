# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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

normas_silver = 'mv_normas_parseadas'
sectores_silver = 'mv_sectores'
table_transform_normas = f'{my_catalog}.silver.{normas_silver}'
table_transform_sectores = f'{my_catalog}.silver.{sectores_silver}'


# COMMAND ----------

# Leer desde Bronze
df_normas_bz = spark.table(table_input_normas_csv)
df_html_bz   = spark.table(table_input_normas_json)
df_sectores_bz   = spark.table(table_input_sectores_csv)

# COMMAND ----------

df_normas_bz.show(2)

# COMMAND ----------

df_normas = (
    df_normas_bz
    .withColumn("id_norma", F.col("id_norma").cast("int"))
    .withColumn("fecha_publicacion", F.to_date("fecha_publicacion", "yyyy-MM-dd"))
    .withColumnRenamed("url", "url_norma")   # 👈 renombramos aquí
)

# COMMAND ----------

df_html_bz.show(2)

# COMMAND ----------

df_html_clean = (
    df_html_bz
    # eliminar tags <...>
    .withColumn("contenido_parseado",
        F.regexp_replace(F.col("html"), "<[^>]+>", " ")
    )
    # colapsar espacios múltiples
    .withColumn("contenido_parseado",
        F.regexp_replace(F.col("contenido_parseado"), "\\s+", " ")
    )
    .drop("html")
    .withColumnRenamed("url", "url_html")    # 👈 renombramos aquí
)

# COMMAND ----------

df_html_clean.show(2)

# COMMAND ----------

# 5) JOIN por id_norma y consolidar
df_normas_parseada = (
    df_normas
    .join(df_html_clean, on="id_norma", how="left")
    .withColumn("titulo",   F.initcap(F.trim(F.col("titulo"))))
    .withColumn("entidad",  F.upper(F.trim(F.col("entidad"))))
    # prioriza url_norma (del CSV); si es nula, usa url_html
    .withColumn("url", F.coalesce(F.col("url_norma"), F.col("url_html")))
    .withColumn(
        "texto_busqueda",
        F.lower(F.coalesce(F.col("contenido_parseado"), F.lit("")))
    )
    .withColumn("fecha_proceso", F.current_timestamp())
    .select(
        "id_norma",
        "titulo",
        "fecha_publicacion",
        "entidad",
        "url",                  # 👈 ya es la final
        "contenido_parseado",
        "texto_busqueda",
        "fecha_proceso"
    )
)

# COMMAND ----------

df_normas_parseada.show()

# COMMAND ----------

df_sectores = (
    df_sectores_bz
    .withColumn("sector_id", F.col("sector_id").cast("int"))
    .withColumn("sector", F.upper(F.trim(F.col("sector"))))
    .withColumn(
        "palabras_array",
        F.split(F.lower(F.col("palabras_clave")), ";")
    )
    .withColumn("fecha_proceso", F.current_timestamp())
)

# COMMAND ----------

df_sectores.show()

# COMMAND ----------

# 3) Grabar en Silver
df_sectores = df_sectores.select(
    "sector_id",
    "sector",
    "palabras_clave",
    "palabras_array",
    "fecha_proceso"
)

# COMMAND ----------

df_normas_parseada.write.format("delta").mode("overwrite").saveAsTable(table_transform_normas)
df_sectores.write.format("delta").mode("overwrite").saveAsTable(table_transform_sectores)

# COMMAND ----------

