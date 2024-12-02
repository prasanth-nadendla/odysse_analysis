# from trip_analysis.module import Get_cluster_data

# obj=Get_cluster_data()
from trip_analysis.analysis.Cluster_analysis.Extract import PocDB_Cluster
from trip_analysis.analysis.Cluster_analysis.Transform import transform_data
from trip_analysis.analysis.Cluster_analysis.Load import DataLoader_Cluster

from trip_analysis.analysis.Airport_analysis.Extract import PocDB_Airport
from trip_analysis.analysis.Airport_analysis.Transform import transform_data
from trip_analysis.analysis.Airport_analysis.Load import DataLoader_Airport

def run_cluster_analysis():
    print("Starting Cluster Analysis ETL...")
    obj=PocDB_Cluster()
    cluster_data = obj.get_table_data()
    transformed_cluster_data = transform_data(cluster_data)
    print(transformed_cluster_data.head(10))
    # DataLoader_Cluster.load_data_to_db(transformed_cluster_data)
    print("Cluster Analysis ETL complete.")

def run_airport_analysis():
    
    print("Starting Airport Analysis ETL...")
    obj=PocDB_Airport()
    airport_data = obj.get_table_data()
    transformed_airport_data = transform_data(airport_data)
    print(transformed_airport_data.head(10))
    # DataLoader_Airport.load_data_to_db(transformed_airport_data)
    print("Airport Analysis ETL complete.")

def main():
    run_airport_analysis()
    # run_cluster_analysis()

if __name__ == "__main__":
    main()