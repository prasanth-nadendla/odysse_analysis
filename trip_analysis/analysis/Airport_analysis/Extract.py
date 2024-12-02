from trip_analysis.config import DbConfig
import psycopg2
from psycopg2 import pool
import pandas as pd

class PocDB_Airport:
    def __init__(self):
        """
        Initialize the AirportPocDB class.
        Sets up the connection pool using the database configuration from DbConfig.
        """
        self.fetch_conf = DbConfig()
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            user=self.fetch_conf.username,
            password=self.fetch_conf.password,
            host=self.fetch_conf.host,
            port=self.fetch_conf.port,
            database=self.fetch_conf.database
        )

    def get_connection(self):
        """
        Acquire a connection from the connection pool.
        """
        return self.connection_pool.getconn()

    def release_connection(self, connection):
        """
        Release a connection back to the pool.
        """
        self.connection_pool.putconn(connection)

    def get_table_data(self) -> pd.DataFrame:
        """
        Retrieve merged data with completed trips and driver details for airport analysis.

        Returns:
            pd.DataFrame: Merged data from multiple tables with relevant columns.
        """
        conn = self.get_connection()
        try:
            query = """
                SELECT 
                    da.id AS id_x, 
                    da."dim_driver_Id", 
                    dd.full_name,
                    da.triprequestdatetime,
                    da.tridroppffdatetime,
                    da.onjobduration_min,
                    da.pickupaddress,
                    da.dropoffaddress,
                    da.tripdistance,
                    da.earnings,
                    da.earningspertriphour,
                    da.earningspertripmile
                FROM fact_aggregatedtrips AS da
                LEFT JOIN dim_driver AS dd 
                ON da."dim_driver_Id" = dd.id
                WHERE da."tripStatus" = 'completed';
            """
            df = pd.read_sql_query(query, conn)
            print(df.head(10))
            print("Data retrieved successfully for airport analysis.")
            return df

        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()
        
        finally:
            self.release_connection(conn)