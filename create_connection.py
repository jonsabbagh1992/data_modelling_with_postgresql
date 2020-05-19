import configparser
import psycopg2

# Constants
CONFIG_FILE = "db.cfg"

def create_database_connection(config_file=CONFIG_FILE):
    """
    Parses config file and returns the connection to the database
    """    
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    
    # Credentials
    HOST = config.get("POSTGRES", "HOST")
    DB_NAME = config.get("POSTGRES", "DB_NAME")
    USER = config.get("POSTGRES", "USER")
    PASSWORD = config.get("POSTGRES", "PASSWORD")
    
    return psycopg2.connect(f"host={HOST} dbname={DB_NAME} user={USER} password={PASSWORD}")