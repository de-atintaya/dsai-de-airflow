#  Pipeline de Ingesta Meteorológica - Grupo 3

Este repositorio contiene la infraestructura y el código (Data Engineering) para la ingesta, validación y transformación de datos de estaciones meteorológicas (`EM_01`, `EM_02`, `EM_03`), orquestado con Apache Airflow y almacenado en Azure Data Lake.

## 🏗️ Arquitectura del Pipeline
El flujo de datos consta de 3 etapas principales orquestadas en un DAG diario:
1. **Validación:** Comprobación local de archivos Excel.
2. **Transformación:** Conversión de `.xlsx` a `.csv` particionado por fecha y estación.
3. **Carga (Cloud):** Ingesta hacia Azure Blob Storage (contenedor `datalake/raw/...`).

*(Ver el diagrama de arquitectura completo en la carpeta `docs/arquitectura_pipeline.png`)*

##  Estructura del Proyecto
```text
PIPELINE_DESARROLLO/
 ├── config/          # Configuraciones de entorno (airflow.cfg)
 ├── dags/            # Scripts de Python con la lógica de orquestación
 ├── data/            # Archivos Excel de origen (Estaciones Meteorológicas)
 ├── docs/            # Diagramas de arquitectura y evidencias de ejecución
 ├── Dockerfile       # Imagen personalizada para dependencias (openpyxl, pandas)
 ├── docker-compose.yaml # Orquestación de contenedores Airflow
 └── README.md