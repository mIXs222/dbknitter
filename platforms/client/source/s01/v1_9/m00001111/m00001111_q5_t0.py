import os
import pymysql
import pandas as pd
from pymongo import MongoClient
from pymongo import errors as mongo_errors

hostname_mysql = "mysql"
username_mysql = "root"
password_mysql = "my-secret-pw"
database_mysql = 'tpch'

hostname_mongo = 'mongodb'
database_mongo = 'tpch'
port_mongo = 27017


def read_mysql(query, conn):
    try:
        data = pd.read_sql(query, conn)
        return data
    except Exception as e:
        print(f"Error: '{e}'")


def read_mongo(collection, query, db):
    try:
        data = pd.DataFrame(list(db[collection].find(query)))
        return data
    except mongo_errors.PyMongoError as e:
        print(f"Could not connect to MongoDB: {e}")


# Create connection to MySQL

conn = pymysql.connect(host=hostname_mysql,
                           user=username_mysql,
                           password=password_mysql,
                           charset='utf8mb4',
                           db=database_mysql)

# Create connection to MongoDB
client = MongoClient(host=hostname_mongo, port=port_mongo)
db = client[database_mongo]

# Fetch data from MySQL
supplier = read_mysql("SELECT * FROM SUPPLIER", conn)
nation = read_mysql("SELECT * FROM NATION", conn)
region = read_mysql("SELECT * FROM REGION", conn)
part = read_mysql("SELECT * FROM PART", conn)

# Fetch data from MongoDB
partsupp = read_mongo("partsupp", {}, db)
customer = read_mongo("customer", {}, db)
orders = read_mongo("orders", {}, db)
lineitem = read_mongo("lineitem", {}, db)

# Close connection to MySQL
conn.close()

# Merge tables
merged = (
    customer.merge(orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(supplier, left_on=['C_NATIONKEY', 'L_SUPPKEY'], right_on=['S_NATIONKEY', 'S_SUPPKEY'])
    .merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Filter data
asia = merged[
    (merged['R_NAME'] == "ASIA")
    & (merged['O_ORDERDATE'] >= '1990-01-01')
    & (merged['O_ORDERDATE'] < '1995-01-01')
]

# GroupBy and sort data
grouped = (
    asia.groupby('N_NAME')
    .apply(lambda x: sum(x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])))
    .reset_index()
)
grouped.columns = ['N_NAME', 'REVENUE']
grouped = grouped.sort_values('REVENUE', ascending=False)

grouped.to_csv('query_output.csv', index=False)
