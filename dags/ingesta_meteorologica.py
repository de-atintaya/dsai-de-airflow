from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# --- CONFIGURACIÓN DE CONSTANTES (ESTRUCTURA DE TU REPOSITORIO) ---
CONTAINER_NAME = "datalake"
WASB_CONN_ID = "utec_blob_storage"
# La data está fuera de dags, por lo que Docker la monta en esta ruta interna:
LOCAL_DATA_DIR = "/opt/airflow/data" 

default_args = {
    'owner': 'grupo_3',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id="g3_ingesta_meteorologica",
    description="Pipeline de ingesta y transformación mínima de estaciones meteorológicas (Grupo 3)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=timezone("America/Bogota")), # Zona horaria local (GMT-5)
    schedule="@daily", # Frecuencia horaria requerida para meteorología
    catchup=False,
    tags=["utec", "g3", "meteorologia", "raw"],
)

def meteorologia_dag():

    @task(task_id="validate_files")
    def validate_files(file_name):

        ruta_local_excel = f"{LOCAL_DATA_DIR}/{file_name}.xlsx"

        if not os.path.exists(ruta_local_excel):
            raise FileNotFoundError(
                f"Archivo no encontrado: {ruta_local_excel}"
            )

        if os.path.getsize(ruta_local_excel) == 0:
            raise ValueError(
                f"Archivo vacío: {ruta_local_excel}"
            )

        logging.info(f"Archivo validado: {ruta_local_excel}")

        return file_name

    @task(task_id="create_raw_paths")
    def create_raw_paths(file_name):

        ruta_local_excel = f"{LOCAL_DATA_DIR}/{file_name}.xlsx"

        ahora = datetime.now()

        fecha_carga = ahora.strftime("%Y-%m-%d")
        timestamp_archivo = ahora.strftime("%Y%m%d_%H%M")

        nombre_csv_final = f"{file_name}_{timestamp_archivo}.csv"
        ruta_local_csv = f"{LOCAL_DATA_DIR}/{nombre_csv_final}"

        logging.info(f"Leyendo archivo: {ruta_local_excel}")

        try:
            df = pd.read_excel(ruta_local_excel)
        except Exception as e:
            raise ValueError(
                f"Formato inválido o archivo corrupto: {ruta_local_excel}"
            ) from e

        df.to_csv(
            ruta_local_csv,
            index=False,
            encoding="utf-8"
        )

        # ==========================================================
        # ESTRUCTURA
        # raw/
        # └── meteorologia/
        #     └── estacion=EM_01/
        #         └── fecha_carga=2026-06-06/
        #             └── EM_01_20260606_1200.csv
        # ==========================================================

        blob_path = (
            f"raw/meteorologia/"
            f"estacion={file_name}/"
            f"fecha_carga={fecha_carga}/"
            f"{nombre_csv_final}"
        )

        logging.info(f"Ruta Raw creada: {blob_path}")

        return {
            "file_name": file_name,
            "csv_path": ruta_local_csv,
            "blob_path": blob_path,
        }

    @task(task_id="upload_files")
    def upload_files(info):

        logging.info(
            f"Conectando a Azure Storage ({WASB_CONN_ID})"
        )

        try:
            wasb_hook = WasbHook(
                wasb_conn_id=WASB_CONN_ID
            )

            with open(info["csv_path"], "rb") as data:

                wasb_hook.upload(
                    container_name=CONTAINER_NAME,
                    blob_name=info["blob_path"],
                    data=data,
                    overwrite=True,
                )

        except Exception as e:
            raise ConnectionError(
                "No se pudo conectar o cargar archivo en Azure Data Lake"
            ) from e

        if os.path.exists(info["csv_path"]):
            os.remove(info["csv_path"])

        logging.info(
            f"Archivo {info['file_name']} cargado correctamente"
        )

    # EM_01
    em01 = validate_files("EM_01")
    em01_raw = create_raw_paths(em01)
    upload_files(em01_raw)

    # EM_02
    em02 = validate_files("EM_02")
    em02_raw = create_raw_paths(em02)
    upload_files(em02_raw)

    # EM_03
    em03 = validate_files("EM_03")
    em03_raw = create_raw_paths(em03)
    upload_files(em03_raw)


dag = meteorologia_dag()
