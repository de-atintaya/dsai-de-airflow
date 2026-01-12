from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import logging
import os

log = logging.getLogger("airflow.task")


def upload_to_adls(
    local_file_path, container_name, blob_name, wasb_conn_id="azure_blob_storage"
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
            overwrite=True,
        )

        log.info(f"==> Uploaded {local_file_path} to {container_name}/{blob_name}")
    except Exception as e:
        log.error("==> Failed to upload to Azure Blob", exc_info=True)
        raise


def upload_delta_lake_to_adls(
    local_delta_table_path,
    container_name,
    remote_delta_table_name,
    wasb_conn_id="azure_blob_storage",
):
    """
    Uploads all files from a local Delta Lake table directory to a specified
    location in Azure Data Lake Storage. This function first deletes any
    existing blobs in the remote directory to ensure a clean overwrite.
    """
    try:
        if not os.path.isdir(local_delta_table_path):
            log.warning(f"==> Local directory not found: {local_delta_table_path}")
            return

        hook = WasbHook(wasb_conn_id=wasb_conn_id)

        # Get the blob service client to delete existing blobs
        blob_service_client = hook.get_conn()
        container_client = blob_service_client.get_container_client(container_name)

        # List all blobs with the given prefix
        blob_list = container_client.list_blobs(
            name_starts_with=remote_delta_table_name
        )

        # Create a list of blob names and sort it in reverse order.
        # This ensures that files are deleted before their parent directories,
        # avoiding the DirectoryNotEmpty error in hierarchical namespaces.
        blobs_to_delete = sorted([blob.name for blob in blob_list], reverse=True)

        if not blobs_to_delete:
            log.info("No existing blobs to delete.")
        else:
            log.info(f"Found {len(blobs_to_delete)} blobs to delete.")
            # Delete blobs one by one in reverse order
            for blob_name in blobs_to_delete:
                log.info(f"Deleting blob: {blob_name}")
                container_client.delete_blob(blob_name)
            log.info("Deletion of existing blobs complete.")

        # Proceed with uploading the new files
        for root, _, files in os.walk(local_delta_table_path):
            for file in files:
                local_path = os.path.join(root, file)
                # Create a relative path to maintain directory structure in the blob
                relative_path = os.path.relpath(local_path, local_delta_table_path)
                blob_name = os.path.join(
                    remote_delta_table_name, relative_path
                ).replace("\\", "/")

                hook.load_file(
                    file_path=local_path,
                    container_name=container_name,
                    blob_name=blob_name,
                    overwrite=True,  # This is still good to have
                )
                log.info(f"==> Uploaded {local_path} to {container_name}/{blob_name}")

        log.info(
            f"==> Successfully uploaded Delta Lake table {local_delta_table_path} to {container_name}/{remote_delta_table_name}"
        )

    except Exception as e:
        log.error("==> Failed to upload Delta Lake table to Azure Blob", exc_info=True)
        raise
