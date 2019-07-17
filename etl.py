import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Runs the queries that copy the data from the S3 repo and stores the contents in staging tables in Redshift
    
    Keyword arguments:
    cur -- the sparkify DB cursor
    conn -- the sparkify DB connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Runs the queries that transform the data from the staging tables and inserts it into the final tables
    
    Keyword arguments:
    cur -- the sparkify DB cursor
    conn -- the sparkify DB connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Coordinates the ETL process for this script
    """
    
    # load the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connects to the Redshift database based on the config
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # loads the stagiung tables
    load_staging_tables(cur, conn)
    
    # loads the final tables
    insert_tables(cur, conn)
    
    # closes the connection
    conn.close()


if __name__ == "__main__":
    main()