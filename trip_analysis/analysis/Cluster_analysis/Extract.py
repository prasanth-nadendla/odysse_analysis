from trip_analysis.config import DbConfig
import psycopg2
from psycopg2 import pool
import pandas as pd

class PocDB_Cluster:
    def __init__(self):
        """
        Initialize the PocDB class.
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
        Retrieve merged data with completed trips and driver details.

        Returns:
            pd.DataFrame: Merged data from multiple tables.
        """
        conn = self.get_connection()
        try:
            query = """
                SELECT 
                    tm.fact_aggregatedtrips_id, 
                    tm.vehicle_reg, 
                    tm.vehicle_id,
                    da.id,
                    dd.full_name,
                    tm.start_latitude,
                    tm.start_longitude,
                    tm.end_latitude,
                    tm.end_longitude,
                    da.triprequestdatetime,
                    da.tridroppffdatetime,
                    da.onjobduration_min,
                    da.pickupaddress,
                    da.dropoffaddress,
                    da.tripdistance,
                    da.earnings,
                    da.earningspertriphour,
                    da.earningspertripmile,
                    tm.source
                FROM trips_mapping AS tm
                INNER JOIN fact_aggregatedtrips AS da ON tm.fact_aggregatedtrips_id = da.id
                INNER JOIN dim_driver AS dd ON da."dim_driver_Id" = dd.id
                WHERE da."tripStatus" = 'completed';
            """
            df = pd.read_sql_query(query, conn)
            print("Data retrieved successfully.")
            return df

        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()
        
        finally:
            self.release_connection(conn)
