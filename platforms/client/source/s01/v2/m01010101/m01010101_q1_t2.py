#!/usr/bin/python3

from pymongo import MongoClient
import mysql.connector
import pandas as pd

def connect_mongodb(database_name, host, port):
    client = MongoClient(host, port)
    db = client[database_name]
    return db

def connect_mysql(database_name, host, port):
    mydb = mysql.connector.connect(
      host=host,
      user="yourusername",
      password="yourpassword",
      database=database_name
    )
    return mydb.cursor()

def query_mongodb(db, table_name):
    mongo_data = pd.DataFrame(list(db[table_name].find()))
    return mongo_data

def query_mysql(mysql_curs, table_name):
    mysql_curs.execute(f"SELECT * FROM {table_name}")
    mysql_data = mysql_curs.fetchall()
    return pd.DataFrame(mysql_data)

def main():
    mongo_db = connect_mongodb('tpch', 'mongodb', 27017)
    mongodb_data = query_mongodb(mongo_db, 'lineitem')

    mysql_cursor = connect_mysql('tpch', 'mysql-host', 3306)
    mysql_data = query_mysql(mysql_cursor, 'lineitem')
    
    # Combine the data from two sources
    combined_data = pd.concat([mongodb_data, mysql_data], ignore_index=True)

    # Query the data
    result = combined_data[
        combined_data['L_SHIPDATE'] <= '1998-09-02'
    ].groupby(
        ['L_RETURNFLAG', 'L_LINESTATUS']
    ).agg({
        'L_QUANTITY': ['sum', 'mean'],
        'L_EXTENDEDPRICE': ['sum', 'mean'],
        'L_DISCOUNT': ['mean'],
        'L_LINESTATUS': ['count'],
        'L_SUM_CHARGE': ['sum'],
        'L_SUM_DISC_PRICE': ['sum']
    }).reset_index()
    
    # Write the output to CSV
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
