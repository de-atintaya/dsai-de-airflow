from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import logging

log = logging.getLogger(__name__)


def list_azure_containers(wasb_conn_id: str):
    """
    Lists all containers in the Azure Storage account associated with the
    given Airflow connection ID.

    :param wasb_conn_id: The Airflow connection ID for Azure Blob Storage.
    :return: A list of container names, or None if an error occurs.
    """
    log.info(f"Connecting to Azure with connection ID: {wasb_conn_id}")
    try:
        # 1. Instantiate the hook with your connection ID
        hook = WasbHook(wasb_conn_id=wasb_conn_id)

        # 2. Get the underlying BlobServiceClient object
        blob_service_client = hook.get_conn()

        log.info("Successfully connected. Fetching container list...")

        # 3. List the containers
        containers = blob_service_client.list_containers()

        # Extract just the names
        container_names = [container["name"] for container in containers]

        if not container_names:
            log.info("No containers found in the storage account.")
        else:
            log.info("Found the following containers:")
            for name in container_names:
                log.info(f"- {name}")

        return container_names

    except Exception as e:
        log.error(f"An error occurred: {e}", exc_info=True)
        return None


# Example of how to use this function in a standalone script
if __name__ == "__main__":
    # NOTE: This part is for local testing. It requires an environment where
    # Airflow is installed and the connection is configured (e.g., via
    # environment variables).

    logging.basicConfig(level=logging.INFO)

    # Replace with the Conn Id you want to inspect
    my_conn_id = "proyecto_azure_blob_storage"

    print(f"Listing containers for connection: {my_conn_id}")
    list_azure_containers(wasb_conn_id=my_conn_id)
