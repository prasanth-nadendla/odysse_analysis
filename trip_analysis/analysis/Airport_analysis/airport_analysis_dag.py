from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

sys.path.append("/opt/airflow/dags/utils/odysse_poc_v2")
from trip_analysis.analysis.Airport_analysis.Extract import PocDB_Airport
from trip_analysis.analysis.Airport_analysis.Transform import transform_data
from trip_analysis.analysis.Airport_analysis.Load import DataLoader_Airport

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the Airport Analysis DAG
@dag(
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="30 12 * * *",  # Run daily at 6:30 PM
    catchup=False,
    tags=["airport_analysis"],
)
def airport_analysis():

    # @task
    # def debug_directory_access():
    #     directory = "/home/walkingtree/odysse_v2/dags/output/trasform_data"
    #     print(f"Checking access for: {directory}")
    #     print(f"Path exists: {os.path.exists(directory)}")
    #     print(f"Writable: {os.access(directory, os.W_OK)}")
    #     print(f"Readable: {os.access(directory, os.R_OK)}")
    #     # Create the directory if it doesn't exist
    #     if not os.path.exists(directory):
    #         os.makedirs(directory, exist_ok=True)
    #         print(f"Directory created: {directory}")

    # Task 1: Extract data
    @task
    def extract_data():
        print("Extracting data for Airport Analysis...")
        obj = PocDB_Airport()
        airport_data = obj.get_table_data()
        print("Data extraction complete.")
        return airport_data

    # Task 2: Transform data
    @task
    def transform_data_task(airport_data):
        print("Transforming data for Airport Analysis...")
        transformed_data = transform_data(airport_data)
        print("Data transformation completed for airport analysis.")

        # Define path to save transformed data as CSV with timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        file_path = f"transformed_data_{timestamp}.csv"

        # if not os.path.exists(file_path):
        #     os.makedirs(file_path, exist_ok=True)
        print(f"file created: {file_path}")
        # Save transformed data to CSV file
        transformed_data.to_csv(file_path, index=False)
        print("Data saved to CSV at:", file_path)
        return file_path


    # Task 3: Load data
    @task
    def load_data(file_path):
        print("Loading data for Airport Analysis...")
        transformed_data = pd.read_csv(file_path)
        loader = DataLoader_Airport()
        loader.load_data_to_db(transformed_data)
        print("Data loading complete.")

    # Define task dependencies using `>>`
    #debug_task = debug_directory_access()
    extract_task = extract_data()
    transform_task = transform_data_task(extract_task)
    load_task = load_data(transform_task)

    # Explicitly set task dependencies
    #debug_task >> 
    extract_task >> transform_task >> load_task

# Instantiate the DAG
airport_analysis_dag_instance = airport_analysis()
