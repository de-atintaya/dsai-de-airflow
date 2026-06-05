from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime, timedelta
import pandas as pd
import os
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
    schedule="@hourly", # Frecuencia horaria requerida para meteorología
    catchup=False,
    tags=["utec", "g3", "meteorologia", "raw"],
)
def meteorologia_dag():

    # Definimos el proceso técnico como una tarea nativa de Airflow
    @task(task_id="procesar_y_subir_estacion")
    def procesar_estacion(file_name: str):
        import os
        print("Instalando dependencia faltante openpyxl...")
        os.system("pip install openpyxl")
        
        ruta_local_excel = f"{LOCAL_DATA_DIR}/{file_name}.xlsx"
        
        # 1. Captura de tiempos actuales para el particionado dinámico
        ahora = datetime.now()
        anio, mes, dia = ahora.strftime("%Y"), ahora.strftime("%m"), ahora.strftime("%d")
        timestamp_archivo = ahora.strftime("%Y%m%d_%H%M")
        
        nombre_csv_final = f"{file_name}_{timestamp_archivo}.csv"
        ruta_local_csv = f"{LOCAL_DATA_DIR}/{nombre_csv_final}"

        print(f"Iniciando lectura de: {ruta_local_excel}")
        df = pd.read_excel(ruta_local_excel)

        # ==============================================================
        # TRANSFORMACIÓN 1: Normalización de Columnas (Snake Case)
        # ==============================================================
        diccionario_columnas = {
            'Estación': 'estacion_id',
            'Fecha': 'fecha',
            'Temperatura (°C)': 'temperatura_c',
            'Temperatura.Max (°C)': 'temperatura_max_c',
            'Temperatura.Min (°C)': 'temperatura_min_c',
            'Velocidad.viento (m/s)': 'velocidad_viento_ms',
            'Velocidad.viento.Max (m/s)': 'velocidad_viento_max_ms',
            'Dirección.viento (°)': 'direccion_viento_deg',
            'Dirección.viento.Moda (°)': 'direccion_viento_moda_deg',
            'Humedad (%)': 'humedad_pct',
            'Humedad.Max (%)': 'humedad_max_pct',
            'Humedad.Min (%)': 'humedad_min_pct',
            'Precipitación (mm)': 'precipitacion_mm',
            'Evaporación (mm)': 'evaporacion_mm',
            'Presión (mbar)': 'presion_mbar',
            'Radiación (W/m2)': 'radiacion_wm2'
        }
        df = df.rename(columns=diccionario_columnas)

        # ==============================================================
        # TRANSFORMACIÓN 2: Cambio de Formato Estructural (Excel -> CSV)
        # ==============================================================
        df.to_csv(ruta_local_csv, index=False, encoding='utf-8')
        print(f"Conversión técnica a CSV completada de manera exitosa: {nombre_csv_final}")

        # ==============================================================
        # TRANSFORMACIÓN 3: Nomenclatura y Particionado Externo (Data Lake)
        # ==============================================================
        # Estructura final en Azure: raw/meteorologia/G3/AÑO/MES/DÍA/archivo.csv
        blob_path = f"raw/meteorologia/G3/{anio}/{mes}/{dia}/{nombre_csv_final}"
        
        print(f"Conectando a Azure Storage (Conexión: {WASB_CONN_ID})")
        wasb_hook = WasbHook(wasb_conn_id=WASB_CONN_ID)
        
        print(f"Subiendo archivo a contenedor '{CONTAINER_NAME}' con ruta: {blob_path}")
        with open(ruta_local_csv, 'rb') as data:
            wasb_hook.upload(
                container_name=CONTAINER_NAME,
                blob_name=blob_path,
                data=data,
                overwrite=True
            )
        
        # Limpieza higiene local: eliminamos el CSV temporal para no saturar el servidor perimetral
        os.remove(ruta_local_csv)
        print(f"¡Estación {file_name} procesada, particionada e ingastada en el Data Lake con éxito!")

    # --- FLUJO DE EJECUCIÓN (Paralelismo Nativo) ---
    # Al llamar las tres tareas de forma independiente, Airflow las ejecuta EN PARALELO de forma automática
    procesar_estacion("EM_01")
    procesar_estacion("EM_02")
    procesar_estacion("EM_03")

# Instanciamos el objeto global del DAG
dag = meteorologia_dag()