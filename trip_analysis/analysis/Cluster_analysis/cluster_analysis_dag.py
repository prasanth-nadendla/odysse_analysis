from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
import os

# Adding project path to system path for imports
sys.path.append("/opt/airflow/dags/utils/odysse_poc_v2")

from trip_analysis.analysis.Cluster_analysis.Extract import PocDB_Cluster
from trip_analysis.analysis.Cluster_analysis.Transform import transform_data_cluster as transform_data
from trip_analysis.analysis.Cluster_analysis.Load import DataLoader_Cluster

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the Cluster Analysis DAG
@dag(
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="00 13 * * *",  # Run daily at 6:30 PM
    catchup=False,
    tags=["cluster_analysis"],
)
def cluster_analysis_dag():

    @task
    def extract_data():
        print("Extracting data for Cluster Analysis...")
        obj = PocDB_Cluster()
        cluster_data = obj.get_table_data()
        print("Data extraction complete. Extracted Data: ", cluster_data.head())  # Debugging info
        return cluster_data

    @task
    def transform_data_task(cluster_data):
        print("Transforming data for Cluster Analysis...")
        print("Data before transformation: ", cluster_data.head())  # Debugging info
        transformed_data = transform_data(cluster_data)
        print("Data after transformation: ", transformed_data.head())  # Debugging info
        return transformed_data

    @task
    def load_data(transformed_data):
        print("Loading data for Cluster Analysis...")
        loader = DataLoader_Cluster()
        loader.load_data_to_db(transformed_data)
        print("Data loading complete.")

    # Define task dependencies
    extract_task = extract_data()
    transform_task = transform_data_task(extract_task)
    load_task = load_data(transform_task)

    extract_task >> transform_task >> load_task

# Instantiate the DAG
cluster_analysis_dag_instance = cluster_analysis_dag()
