
from trip_analysis.config import DbConfig
import psycopg2
from .get_csv import transform_df
from psycopg2 import pool
import pandas as pd
from datetime import date,timedelta


class PocDB:
    def __init__(self):
        """
        Initialize the PostgresDB class.
        This method sets up the connection pool using the database configuration
        provided by the DbConfig class.
        """
        self.fetch_conf=DbConfig()
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,  # Minimum number of connections
            maxconn=10,  # Maximum number of connections
            user=self.fetch_conf.username,
            password=self.fetch_conf.password,
            host=self.fetch_conf.host,
            port=self.fetch_conf.port,
            database=self.fetch_conf.database
        )
        

    def get_connection(self):
        """
        Acquire a connection from the connection pool.
        This method returns an active connection from the pool.
        """
        return self.connection_pool.getconn()

    def release_connection(self, connection):
        """
        Return the connection to the pool.
        This method releases a connection back into the pool after use.
        """
        self.connection_pool.putconn(connection)

    # def close_all_connections(self):
    #     """
    #     Close all connections in the connection pool.
    #     This method closes all connections when they are no longer needed.
    #     """
    #     self.connection_pool.closeall()

    def get_table_data(self,table_name)->pd.DataFrame:
        """
        Retrieve a list of unique KPI (Key Performance Indicator) keys from the database.
        This method executes a SQL query to select distinct KPI keys from the specified table.
        Returns:
            A list of unique KPI keys, or a message if no indicators are found.
        """
        conn = self.get_connection()
        res_path=''
        try:
            # Use pandas to read the entire table as a DataFrame
            # table_name = self.fetch_conf.trips_mapping
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, conn)
            print(df.head(1))
            
            res_path = transform_df(df, table_name)
            
    
        finally:
            self.release_connection(conn)
            return res_path


    # def get_fact_aggregated_trip(self,) -> pd.DataFrame:
    #     """
    #     Retrieve the code for a specific technical indicator from the database.
    #     This method fetches the code for a given KPI key (indicator name).
    #     Args:
    #         indicator_name: The name of the technical indicator to retrieve.
    #     Returns:
    #         The code corresponding to the indicator, or a message if not found.
    #     """
    #     conn = self.get_connection()
    #     try:
    #         # Use pandas to read the entire table as a DataFrame
    #         table_name = self.fetch_conf.fact_agregatedtrips
    #         query = f"SELECT * FROM {table_name}"
            
    #         # Fetch the entire table into a DataFrame
    #         df = pd.read_sql_query(query, conn)
            
    #         # Print the first row of the DataFrame
    #         print(df.head(1))
            
    #         # Optionally: Save the DataFrame if needed
    #         res_path = transform_df(df, table_name)
            
    
    #     finally:
    #         self.release_connection(conn)
    #         return res_path

    