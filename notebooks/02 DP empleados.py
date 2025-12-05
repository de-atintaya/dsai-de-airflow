# Databricks notebook source
from pyspark.sql.functions import regexp_replace, to_date, col
from pyspark.sql import functions as F

# COMMAND ----------

my_catalog = 'g10_tal_colaboradores'
my_prefix = 'mv_empleados_normalizado'

# COMMAND ----------

table_input = f'{my_catalog}.bronze.empleados'
table_transform = f'{my_catalog}.silver.{my_prefix}'

# COMMAND ----------

df_bronze = spark.table(table_input)


# COMMAND ----------

# Limpieza básica de nombre y apellido
df_silver = (
    df_bronze
    .withColumn("nombre",   F.initcap(F.trim(F.col("nombre"))))
    .withColumn("apellido", F.initcap(F.trim(F.col("apellido"))))
    .withColumn("area_raw", F.upper(F.trim(F.col("area"))))
    .drop("area")
)

# Normalización de áreas
df_silver = (
    df_silver
    .withColumn(
        "area",
        F.when(F.col("area_raw").isin("RIESGOS"), "RIESGOS")
         .when(F.col("area_raw").isin("TI", "TECNOLOGIA"), "TECNOLOGIA")
         .when(F.col("area_raw").isin("FINANZAS"), "FINANZAS")
         .when(F.col("area_raw").isin("RRHH", "RECURSOS HUMANOS"), "RECURSOS HUMANOS")
         .when(F.col("area_raw").isin("OPERACIONES"), "OPERACIONES")
         .when(F.col("area_raw").isin("MARKETING", "MKT"), "MARKETING")
         .when(F.col("area_raw").like("DATA%"), "DATA & ANALYTICS")
         .otherwise("OTROS")
    )
    .drop("area_raw")
)

# Campos técnicos / de negocio adicionales
df_silver = (
    df_silver
    .withColumn("empleado_id", F.monotonically_increasing_id())
    .withColumn("nombre_completo", F.concat_ws(" ", "nombre", "apellido"))
    .withColumn("fecha_proceso", F.current_date())
)

# COMMAND ----------

df_silver.show()

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").saveAsTable(table_transform)

# COMMAND ----------

