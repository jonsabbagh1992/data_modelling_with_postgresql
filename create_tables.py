import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Constants
CONFIG_FILE = "db.cfg"

def create_database_connection(config_file=CONFIG_FILE):
    """
    - Connects to the database
    - Returns the connection and cursor to sparkifydb
    """    
    import configparser

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    
    # Credentials
    HOST = config.get("POSTGRES", "HOST")
    DB_NAME = config.get("POSTGRES", "DB_NAME")
    USER = config.get("POSTGRES", "USER")
    PASSWORD = config.get("POSTGRES", "PASSWORD")
    
    # connect to sparkify database
    conn = psycopg2.connect(f"host={HOST} dbname={DB_NAME} user={USER} password={PASSWORD}")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database_connection()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()