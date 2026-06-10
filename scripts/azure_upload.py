from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import logging
import os

log = logging.getLogger('airflow.task')

def upload_to_adls(
        local_file_path = "/opt/airflow/data/sample.txt",
        container_name = "airflow",
        blob_name = "raw/uploaded_sample.txt",
        wasb_conn_id = "azure_blob_storage"
        ):
    try:
        if not os.path.exists(local_file_path):
            log.warning(f"==> Local file not found: {local_file_path}")
            return
        
        # Connect using the connection created in UI
        hook = WasbHook(wasb_conn_id=wasb_conn_id)    
        hook.load_file(
            file_path=local_file_path,
            container_name=container_name,
            blob_name=blob_name,
            overwrite=True
        )

        log.info(f"==> Uploaded {local_file_path} to {container_name}/{blob_name}")
    except Exception as e:
        log.error("==> Failed to upload to Azure Blob", exc_info=True)
        raise

def upload_folder_to_adls(
        local_folder_path,
        container_name = "airflow",
        blob_prefix = "raw/G1",
        wasb_conn_id = "azure_blob_storage"
        ):
    if not os.path.exists(local_folder_path):
        log.warning(f"==> Local folder not found: {local_folder_path}")
        return

    hook = WasbHook(wasb_conn_id=wasb_conn_id)
    
    for root, dirs, files in os.walk(local_folder_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder_path)
            blob_name = os.path.join(blob_prefix, relative_path).replace("\\", "/")

            try:
                log.info(f"==> Uploading {local_file_path} to {container_name}/{blob_name}")
                hook.load_file(
                    file_path=local_file_path,
                    container_name=container_name,
                    blob_name=blob_name,
                    overwrite=True
                )
            except Exception as e:
                log.error(f"==> Failed to upload {file} to Azure Blob", exc_info=True)
                raise