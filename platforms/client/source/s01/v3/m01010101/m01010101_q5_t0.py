# bash commands to install dependencies
# pip install mysql-connector-python
# pip install pymongo
# pip install pandas

import os
import pandas as pd
import mysql.connector
from pymongo import MongoClient

def get_data_mysql():
    connection = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', 
                                         database='tpch')
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM nation')
    nation = cursor.fetchall()
    cursor.execute('SELECT * FROM orders')
    orders = cursor.fetchall()
    connection.close()

    return nation, orders

def get_data_mongodb():
    connection = MongoClient('mongodb', 27017)
    mongodb = connection['tpch']
    region = list(mongodb['region'].find())
    supplier = list(mongodb['supplier'].find())
    customer = list(mongodb['customer'].find())
    lineitem = list(mongodb['lineitem'].find())
    return region, supplier, customer, lineitem

data_mysql = get_data_mysql()
data_mongo = get_data_mongodb()

# Transform data into pandas DataFrame
nation_df = pd.DataFrame(data_mysql[0], columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
orders_df = pd.DataFrame(data_mysql[1], columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

region_df = pd.DataFrame(data_mongo[0])
supplier_df = pd.DataFrame(data_mongo[1])
customer_df = pd.DataFrame(data_mongo[2])
lineitem_df = pd.DataFrame(data_mongo[3])

# Join all tables
joined_tables = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
joined_tables = joined_tables.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
joined_tables = joined_tables.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
joined_tables = joined_tables.merge(nation_df, left_on=['C_NATIONKEY', 'S_NATIONKEY'], right_on=['N_NATIONKEY', 'N_NATIONKEY'])
joined_tables = joined_tables.merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Query data
query_result = joined_tables.query('R_NAME == "ASIA" and O_ORDERDATE >= "1990-01-01" and O_ORDERDATE < "1995-01-01"')
query_result = query_result.groupby('N_NAME').apply(lambda x: sum(x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])))
query_result = query_result.sort_values(ascending=False)

# Write output to a CSV file
query_result.to_csv('query_output.csv')
