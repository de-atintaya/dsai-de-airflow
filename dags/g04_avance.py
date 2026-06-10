# Parcial (avance): Mueve archivos locales a Azure Blob Storage (Datalake) usando Airflow
# Por ahora solo se subirá el archivo: tienda_1001_2026_06_08.csv

"""
    Grupo: 04
    Integrantes:
        - Christian Cordova
        - Yesseliz Choque
        - Meliza Sosa
        - Kevin Zevallos

DAG: dag_ingesta_envios
Descripción: Ingesta de un archivo CSV de tienda hacia el contenedor
             Raw en Azure Data Lake Storage via WASB connection.
 
Archivo fuente: tienda_1001_2026_06_08.csv
"""

from airflow.decorators import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

from datetime import datetime, timedelta
from pendulum import timezone

import logging
import os
import csv


# ─────────────────────────────────────────────
# CONFIGURACIÓN GENERAL
# ─────────────────────────────────────────────
LOCAL_FILE_PATH = "/opt/airflow/data/tienda_1001_2026_06_08.csv"
CONTAINER_NAME = "datalake"  # in Azure
WASB_CONN_ID = "utec_blob_storage"
BLOB_NAME = "raw/airflow2/G04/tienda_1001_2026_06_08_subido.csv"

# Columnas que se esperan en el archivo CSV
COLUMNAS_ESPERADAS = {
    "id_envio",
    "fecha",
    "tienda_origen",
    "tienda_destino",
    "estado",
    "courier_asignado",
    "tiempo_estimado_hrs",
    "tiempo_real_hrs",
}

# Columnas de auditoría que se agregan antes de subir
COLUMNA_FECHA_INGESTA = "fecha_ingesta"
COLUMNA_NOMBRE_ARCHIVO = "nombre_archivo"
COLUMNA_ORIGEN = "origen_ingesta"


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# ─────────────────────────────────────────────
# ARGUMENTOS POR DEFECTO DEL DAG
# ─────────────────────────────────────────────
 
default_args = {
    "owner": "equipo_datos",
    "depends_on_past": False,
    "email": ["christian.cordova@utec.edu.pe", "yesseliz.choque@utec.edu.pe", "meliza.sosa@utec.edu.pe", "kevin.zevallos@utec.edu.pe"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# ─────────────────────────────────────────────
# DEFINICIÓN DEL DAG
# ─────────────────────────────────────────────
 
@dag(
    dag_id="g04_ingesta_envios",
    description="Ingesta de archivo CSV de tienda hacia Azure Data Lake (Raw)",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Lima")),
    schedule="0 12 * * 1", # Runs every monday at 12:00 local time (GMT-5)
    catchup=False,
    tags=["ingesta", "logistica", "azure", "raw", "utec", "blob"],
)
def ingesta_dag():

    @task
    def verificar_archivo():
        """
        Tarea 1: Verifica que el archivo CSV exista en la ruta local.
        """
        if not os.path.exists(LOCAL_FILE_PATH):
            raise FileNotFoundError(
                f"[FALTANTE] Archivo no encontrado: {LOCAL_FILE_PATH}"
            )
 
        logging.info(f"[OK] Archivo encontrado: {LOCAL_FILE_PATH}")
        return LOCAL_FILE_PATH


    @task
    def validar_estructura():
        """
        Tarea 2: Valida que el archivo no esté vacío y contenga
        las columnas esperadas según el schema definido.
        """
        if os.path.getsize(LOCAL_FILE_PATH) == 0:
            raise ValueError(f"El archivo está vacío: {LOCAL_FILE_PATH}")
 
        with open(LOCAL_FILE_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            columnas_archivo = set(reader.fieldnames or [])
            columnas_faltantes = COLUMNAS_ESPERADAS - columnas_archivo
 
            if columnas_faltantes:
                raise ValueError(
                    f"Columnas faltantes en el archivo: {columnas_faltantes}"
                )
 
            registros = sum(1 for _ in reader)
            if registros == 0:
                raise ValueError("El archivo no tiene registros (solo cabecera).")
 
            logging.info(f"[OK] Estructura válida: {registros} registros encontrados.")
            return registros


    @task
    def agregar_metadatos():
        """
        Tarea 3: Agrega columnas de auditoría al archivo antes de subirlo.
        Escribe el archivo enriquecido en una subcarpeta /procesados.
        """
        fecha_ingesta_str = datetime.utcnow().isoformat()
        nombre_archivo    = os.path.basename(LOCAL_FILE_PATH)
        ruta_procesados   = os.path.join(os.path.dirname(LOCAL_FILE_PATH), "procesados")
        os.makedirs(ruta_procesados, exist_ok=True)
 
        ruta_destino = os.path.join(ruta_procesados, nombre_archivo)
 
        with open(LOCAL_FILE_PATH, "r", encoding="utf-8") as f_in, \
             open(ruta_destino, "w", newline="", encoding="utf-8") as f_out:
 
            reader    = csv.DictReader(f_in)
            fieldnames = reader.fieldnames + [
                COLUMNA_FECHA_INGESTA,
                COLUMNA_NOMBRE_ARCHIVO,
                COLUMNA_ORIGEN,
            ]
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
 
            for row in reader:
                row[COLUMNA_FECHA_INGESTA]  = fecha_ingesta_str
                row[COLUMNA_NOMBRE_ARCHIVO] = nombre_archivo
                row[COLUMNA_ORIGEN]         = "tienda_1001"
                writer.writerow(row)
 
        logging.info(f"[OK] Metadatos agregados → {ruta_destino}")
        return ruta_destino


    @task
    def cargar_a_adls(ruta_procesado: str):
        """
        Tarea 4: Sube el archivo enriquecido a Azure Data Lake Storage
        usando el WASB connection configurado en Airflow.
        Verifica duplicados antes de subir.
        """
        hook = WasbHook(wasb_conn_id=WASB_CONN_ID)
 
        # Verificar si ya existe en ADLS (evitar duplicados)
        if hook.check_for_blob(CONTAINER_NAME, BLOB_NAME):
            logging.warning(f"[OMITIDO] {BLOB_NAME} ya existe en {CONTAINER_NAME}. Se omite la carga.")
            return {"subido": False, "blob": BLOB_NAME}
 
        hook.load_file(
            file_path=ruta_procesado,
            container_name=CONTAINER_NAME,
            blob_name=BLOB_NAME,
            overwrite=False,
        )
 
        logging.info(f"[SUBIDO] {ruta_procesado} → {CONTAINER_NAME}/{BLOB_NAME}")
        return {"subido": True, "blob": BLOB_NAME}


    @task
    def notificar_resultado(resultado_carga: dict):
        """
        Tarea 5: Registra en log el resumen de la ejecución.
        """
        estado = "SUBIDO" if resultado_carga.get("subido") else "OMITIDO (ya existía)"
 
        resumen = (
            f"\n{'='*50}\n"
            f"RESUMEN EJECUCIÓN DAG: dag_ingesta_envios\n"
            f"Archivo : {BLOB_NAME}\n"
            f"Estado  : {estado}\n"
            f"{'='*50}"
        )
        logging.info(resumen)


    # ── Definición del flujo ──
    verificar_archivo()
    validar_estructura()
    ruta_procesado = agregar_metadatos()
    resultado_carga = cargar_a_adls(ruta_procesado)
    notificar_resultado(resultado_carga)

dag = ingesta_dag()
