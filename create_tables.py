import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops the tables in the sparkify database.
    
    Runs the list of drop queries from the sql_queries script.
    
    Keyword arguments:
    cur -- the sparkify DB cursor
    conn -- the sparkify DB connection
    """
    
    # drop the tables using the sql_queries.py queries
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates the tables in the sparkify database.
    
    Runs the list of create queries from the sql_queries script.
    
    Keyword arguments:
    cur -- the sparkify DB cursor
    conn -- the sparkify DB connection
    """
    
    # create the tables using the sql_queries.py queries
    for query in create_table_queries:
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

    # dropping the tables
    drop_tables(cur, conn)
    
    # creating the tables
    create_tables(cur, conn)

    # closing the connection
    conn.close()


if __name__ == "__main__":
    main()