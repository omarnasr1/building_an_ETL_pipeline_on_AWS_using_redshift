import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """loads the data in the staging tables"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """insert the data in the tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - read the config file . 
    
    - Establishes connection with the database and gets
     cursor to it.  
    
    - loads the data in the staging tables.  
    
    - insert the data in the tables. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    conn.set_session(autocommit=True)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
