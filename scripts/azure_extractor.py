import pandas as pd
from azure.storage.blob import BlobServiceClient
import logging
import os
from io import StringIO

def leer_datos_azure_blob() -> pd.DataFrame:
  blob_service_client = None

  try:
    account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
    account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
    container_name = os.getenv('AZURE_BLOB_CONTAINER_NAME')
    blob_name = os.getenv('AZURE_BLOB_NAME')  # path/to/file.csv

    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    if not connection_string and (not account_name or not account_key):
      raise ValueError(
          "Either AZURE_STORAGE_CONNECTION_STRING or both AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY are required")
    if not container_name:
      raise ValueError(
          "AZURE_BLOB_CONTAINER_NAME environment variable is required")
    if not blob_name:
      raise ValueError("AZURE_BLOB_NAME environment variable is required")

    if connection_string:
      logging.info("Connecting to Azure Blob Storage using connection string")
      blob_service_client = BlobServiceClient.from_connection_string(
          connection_string)
    else:
      logging.info(f"Connecting to Azure Blob Storage account: {account_name}")
      account_url = f"https://{account_name}.blob.core.windows.net"
      blob_service_client = BlobServiceClient(
          account_url=account_url, credential=account_key)

    container_client = blob_service_client.get_container_client(container_name)

    container_properties = container_client.get_container_properties()
    logging.info(f"Successfully connected to container: {container_name}")

    logging.info(
        f"Downloading blob: {blob_name} from container: {container_name}")

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    blob_data = blob_client.download_blob()
    csv_content = blob_data.readall().decode('utf-8')

    df = pd.read_csv(StringIO(csv_content))

    logging.info(
        f"Successfully extracted {len(df)} records from Azure Blob Storage")
    logging.info(f"Columns: {list(df.columns)}")
    logging.info(f"DataFrame shape: {df.shape}")

    return df

  except Exception as e:
    logging.error(f"Error extracting data from Azure Blob Storage: {str(e)}")
    raise
