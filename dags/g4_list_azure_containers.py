from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime
from scripts.azure_utils import list_azure_containers


@dag(
    dag_id="g4_list_azure_containers",
    description="A utility DAG to list Azure Blob Storage containers.",
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule=None,  # Manually triggered
    catchup=False,
    tags=["utec", "azure", "utility"],
)
def list_containers_dag():
    @task
    def list_my_containers():
        # Replace with the connection ID you want to inspect
        azure_connection_id = "proyecto_azure_blob_storage"
        list_azure_containers(wasb_conn_id=azure_connection_id)

    list_my_containers()


dag = list_containers_dag()
