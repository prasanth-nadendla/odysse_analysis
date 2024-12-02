from dotenv import load_dotenv
import os
load_dotenv()


class DbConfig:
    def __init__(self):
        """
        Initializes the DbConfig class, which is responsible for holding 
        database configuration details. The configuration details are fetched 
        from environment variables.
        """
        self.username = os.getenv("database_user")
        self.host = os.getenv("database_host")
        self.password = os.getenv("database_password")
        self.database = os.getenv("database_name")
        self.port = os.getenv("database_port")
        