"""
Created on  2019-09-07
@author: Jingchao Yang

https://www.mydatahack.com/how-to-bulk-load-data-into-postgresql-with-python/
"""
import psycopg2
import pandas as pd
import sys


def table_to_csv(sql, file_path, dbname, host, port, user, pwd):
    '''
    This function creates a csv file from PostgreSQL with query
    '''
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port, \
                                user=user, password=pwd)
        print("Connecting to Database")
        # Get data into pandas dataframe
        df = pd.read_sql(sql, conn)
        # Write to csv file
        df.to_csv(file_path, encoding='utf-8', header=True, \
                  doublequote=True, sep=',', index=False)
        print("CSV File has been created")
        conn.close()

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)


# Execution Example with transaction table
sql = 'Select * From sf.transaction'
file_path = '/tmp/transaction.csv'
dbname = 'db name'
host = 'host url'
port = '5432'
user = 'username'
pwd = 'password'

table_to_csv(sql, file_path, dbname, host, port, user, pwd)


def pg_load_table(file_path, table_name, dbname, host, port, user, pwd):
    '''
    This function upload csv to a target table
    '''
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port, \
                                user=user, password=pwd)
        print("Connecting to Database")
        cur = conn.cursor()
        f = open(file_path, "r")
        # Truncate the table first
        cur.execute("Truncate {} Cascade;".format(table_name))
        print("Truncated {}".format(table_name))
        # Load table from the file with header
        cur.copy_expert("copy {} from STDIN CSV HEADER QUOTE '\"'".format(table_name), f)
        cur.execute("commit;")
        print("Loaded data into {}".format(table_name))
        conn.close()
        print("DB connection closed.")

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)


# Execution Example
file_path = '/tmp/restaurants.csv'
table_name = 'usermanaged.restaurants'
dbname = 'db name'
host = 'host url'
port = '5432'
user = 'username'
pwd = 'password'
pg_load_table(file_path, table_name, dbname, host, port, user, pwd)
