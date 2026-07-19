# Guía de ingesta Raw y Pull Request — Grupo G202

## 1. Objetivo

Implementar la primera etapa del pipeline de datos del grupo G202 utilizando Apache Airflow. Los DAG toman archivos CSV de carpetas locales simuladas, validan su existencia y esquema mínimo, y los cargan en la zona Raw de Azure Data Lake Storage.

Esta etapa termina con la carga en ADLS Raw. El procesamiento posterior en Databricks no forma parte de este entregable.

## 2. Trabajo realizado

Se crearon y publicaron los siguientes DAG:

- `dags/g202_flt_carbono_ingesta.py`
- `dags/g202_flt_zonas_ingesta.py`

Ambos están en la rama:

```text
feature/dag-g202-ingesta
```

El commit inicial es:

```text
e0e711e Agrega DAGs de ingesta Raw del grupo G202
```

### DAG `g202_flt_carbono_ingesta`

Valida y carga:

- `consumo_ep_2021.csv`
- `descargas_2021.csv`
- `embarcaciones_maestro.csv`

Destino general:

```text
airflow/raw/G202/flt_carbono/<fuente>/<fecha_ejecucion>/<archivo.csv>
```

### DAG `g202_flt_zonas_ingesta`

Valida y carga:

- `vms_2021_w2.csv`
- `esfuerzo_gfw_2021.csv`
- `puertos.csv`

Destino general:

```text
airflow/raw/G202/flt_zonas/<fuente>/<fecha_ejecucion>/<archivo.csv>
```

### Comportamiento implementado

- Verifica que cada archivo local exista.
- Verifica que el CSV tenga las columnas mínimas requeridas.
- Rechaza archivos sin registros.
- Utiliza la conexión de Airflow `azure_blob_storage`.
- Carga los archivos en el contenedor `airflow` mediante `WasbHook`.
- Permite reejecuciones usando `overwrite=True`.
- Se ejecuta manualmente con `schedule=None`.
- No necesita `pandas`, `requests` ni variables de Databricks.

## 3. Validaciones realizadas

Se completaron las siguientes comprobaciones:

```powershell
python -m py_compile dags\g202_flt_carbono_ingesta.py dags\g202_flt_zonas_ingesta.py
docker compose config --quiet
git diff --check
```

Resultados:

- Los dos archivos tienen sintaxis Python válida.
- La configuración de Docker Compose es válida.
- Los DAG tienen identificadores diferentes y no generan conflicto en Airflow.
- Los encabezados de los seis CSV locales coinciden con los esquemas mínimos configurados.

La prueba integrada en Airflow y Azure queda pendiente porque Docker Desktop estaba apagado durante la validación.

## 4. Estado de Git y precauciones

Abrir PowerShell en el repositorio:

```powershell
cd "C:\Users\dge_2\OneDrive\0.Maestria\4. data engineer\proyecto2\dsai-de-airflow"
```

Confirmar la rama actual:

```powershell
git branch --show-current
```

Resultado esperado:

```text
feature/dag-g202-ingesta
```

Revisar el estado:

```powershell
git status
```

No ejecutar `git add .`: existen cambios locales en `.gitignore`, `docker-compose.yaml` y archivos de instrucciones que no deben entrar accidentalmente al Pull Request.

El archivo `intrucciones/azure_connection (1).txt` contiene una credencial y nunca debe agregarse a Git. La clave expuesta debe rotarse antes de utilizar el Storage Account en una prueba real.

## 5. Cómo agregar esta guía a GitHub

Agregar exclusivamente esta guía:

```powershell
git add -- intrucciones/GUIA_G202_PR.md
```

Confirmar exactamente qué archivo está preparado:

```powershell
git diff --cached --name-only
```

Resultado esperado:

```text
intrucciones/GUIA_G202_PR.md
```

Revisar su contenido antes del commit:

```powershell
git diff --cached
```

Crear el commit:

```powershell
git commit -m "Documenta ingesta y Pull Request del grupo G202"
```

Subir el commit a la rama remota:

```powershell
git push origin feature/dag-g202-ingesta
```

Confirmar que la rama quedó sincronizada:

```powershell
git status
```

Debe indicar que la rama está actualizada con `origin/feature/dag-g202-ingesta`.

## 6. Cómo crear manualmente el Pull Request

Abrir en el navegador:

```text
https://github.com/de-atintaya/dsai-de-airflow/compare/develop...feature/dag-g202-ingesta?expand=1
```

Verificar antes de crearlo:

- Repositorio: `de-atintaya/dsai-de-airflow`.
- Rama base: `develop`.
- Rama de comparación: `feature/dag-g202-ingesta`.
- Los archivos modificados deben ser los dos DAG y esta guía.
- No deben aparecer `.env`, archivos de `data/`, el connection string ni `azure_connection (1).txt`.

Usar este título:

```text
DAGs de ingesta Raw del grupo G202
```

Usar esta descripción:

```markdown
## Objetivo

Implementar la primera etapa del pipeline del grupo G202 para cargar archivos CSV locales en Azure Data Lake, dentro de la zona Raw.

## Cambios

- Agrega `g202_flt_carbono_ingesta`.
- Agrega `g202_flt_zonas_ingesta`.
- Valida existencia, esquema mínimo y contenido de los CSV.
- Carga los archivos mediante la conexión `azure_blob_storage`.
- Organiza los blobs bajo `raw/G202/`.
- No incluye procesamiento de Databricks porque está fuera de esta primera etapa.

## Validaciones

- Sintaxis Python verificada.
- Configuración de Docker Compose verificada.
- Pendiente prueba integrada con Docker Desktop, Airflow y Azure.

## Destino

Contenedor `airflow`:

- `raw/G202/flt_carbono/...`
- `raw/G202/flt_zonas/...`
```

Seleccionar **Create pull request**. El laboratorio termina con la creación del PR: no hacer merge.

## 7. Prueba pendiente en Airflow

### 7.1 Iniciar Docker y Airflow

Abrir Docker Desktop y esperar hasta que el motor esté activo. Después ejecutar:

```powershell
docker compose up airflow-init
docker compose up --build -d
docker compose ps
```

Los servicios principales deben aparecer en ejecución. Abrir:

```text
http://localhost:8080
```

Las credenciales predeterminadas del proyecto son:

```text
Usuario: airflow
Contraseña: airflow
```

### 7.2 Configurar la conexión con Azure

En Airflow:

1. Entrar a **Admin > Connections**.
2. Crear o editar la conexión `azure_blob_storage`.
3. Seleccionar el tipo `wasb`.
4. Colocar en **Extra** un JSON con este formato, usando una credencial vigente:

```json
{
  "connection_string": "<CONNECTION_STRING_VIGENTE>"
}
```

5. Guardar la conexión.

No copiar el valor real al DAG, a esta guía, a `.env` versionado ni a ningún commit.

### 7.3 Ejecutar los DAG

En la interfaz de Airflow:

1. Buscar `g202_flt_carbono_ingesta`.
2. Confirmar que no tenga errores de importación.
3. Activarlo y seleccionar **Trigger DAG**.
4. Confirmar que `validate_local_files` y `upload_files_to_raw` terminen correctamente.
5. Repetir el proceso con `g202_flt_zonas_ingesta`.
6. Revisar los logs de `upload_files_to_raw`; deben mostrar una línea `Archivo cargado` por cada CSV.

## 8. Verificación en Azure Data Lake

En Azure Portal o Azure Storage Explorer, abrir el Storage Account configurado y revisar el contenedor `airflow`.

Rutas esperadas:

```text
raw/G202/flt_carbono/consumo_ep/<YYYYMMDD>/consumo_ep_2021.csv
raw/G202/flt_carbono/descargas/<YYYYMMDD>/descargas_2021.csv
raw/G202/flt_carbono/embarcaciones/<YYYYMMDD>/embarcaciones_maestro.csv
raw/G202/flt_zonas/vms/<YYYYMMDD>/vms_2021_w2.csv
raw/G202/flt_zonas/esfuerzo_gfw/<YYYYMMDD>/esfuerzo_gfw_2021.csv
raw/G202/flt_zonas/puertos/<YYYYMMDD>/puertos.csv
```

`YYYYMMDD` corresponde a la fecha lógica de la ejecución del DAG.

Para cada archivo, confirmar:

- Existe en la ruta esperada.
- Tiene tamaño mayor que cero.
- Conserva el nombre del archivo fuente.
- Puede descargarse o previsualizarse como CSV.

## 9. Cierre y criterios de aceptación

El entregable se considera listo cuando:

- Airflow muestra los dos DAG sin errores de importación.
- Ambos DAG finalizan correctamente.
- Los seis CSV aparecen en ADLS Raw.
- La rama remota contiene los dos DAG y esta guía.
- El Pull Request apunta desde `feature/dag-g202-ingesta` hacia `develop`.
- El PR no contiene secretos ni archivos locales de datos.
- El Pull Request queda abierto y no se hace merge.

Al terminar las pruebas locales, apagar Airflow:

```powershell
docker compose down
```

