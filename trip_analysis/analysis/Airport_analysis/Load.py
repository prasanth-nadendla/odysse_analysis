import pandas as pd
import psycopg2
from psycopg2 import pool
from trip_analysis.config import DbConfig

class DataLoader_Airport:
    def __init__(self):
        """
        Initialize the DataLoader class.
        Sets up the connection pool using the database configuration from DbConfig.
        """
        self.db_config = DbConfig()
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            user=self.db_config.username,
            password=self.db_config.password,
            host=self.db_config.host,
            port=self.db_config.port,
            database=self.db_config.database
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

    def load_data_to_db(self, data: pd.DataFrame, table_name: str = 'airport_analysis'):
        """
        Loads a DataFrame into the specified table in the database.

        Args:
            data (pd.DataFrame): The transformed data to be loaded into the database.
            table_name (str): The name of the table to create or replace. Defaults to 'airport_analysis'.
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            columns_with_types = []
            for col, dtype in zip(data.columns, data.dtypes):
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "FLOAT"
                elif pd.api.types.is_bool_dtype(dtype):
                    sql_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sql_type = "TIMESTAMP"
                elif pd.api.types.is_object_dtype(dtype):
                    sql_type = "VARCHAR(255)"
                else:
                    sql_type = "VARCHAR(255)"
                columns_with_types.append(f"{col} {sql_type}")

            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns_with_types)}
            );
            """
            cursor.execute(create_table_query)

            insert_query = f"""
            INSERT INTO {table_name} ({', '.join(data.columns)}) 
            VALUES ({', '.join(['%s' for _ in data.columns])});
            """
            for _, row in data.iterrows():
                cursor.execute(insert_query, tuple(row))

            conn.commit()
            print(f"Data loaded successfully into '{table_name}' table.")

        except Exception as e:
            print(f"Error loading data to the database: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self.release_connection(conn)
