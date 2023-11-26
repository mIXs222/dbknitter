# query.py

from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import direct_redis
import csv

# Connect to MongoDB
def mongo_connect(host, port, db_name):
    client = MongoClient(host, port)
    db = client[db_name]
    return db

def fetch_mongo_data(db, collection_name):
    collection = db[collection_name]
    data = list(collection.find())
    return pd.DataFrame(data)

# Connect to Redis
def redis_connect(host, port, db_name):
    r = direct_redis.DirectRedis(host=host, port=port, db=int(db_name))
    return r

def fetch_redis_data(redis_conn, key_name):
    data = redis_conn.get(key_name)
    df = pd.read_json(data)
    return df

# Convert string date to datetime object
def convert_str_to_date(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d')

# MongoDB connection details
mongo_db_name = 'tpch'
mongo_port = 27017
mongo_host = 'mongodb'

# Redis connection details
redis_db_name = '0'
redis_port = 6379
redis_host = 'redis'

# Connect to the databases
mongo_db = mongo_connect(mongo_host, mongo_port, mongo_db_name)
redis_conn = redis_connect(redis_host, redis_port, redis_db_name)

# Fetch data frames
nation_df = fetch_mongo_data(mongo_db, 'nation')
orders_df = fetch_mongo_data(mongo_db, 'orders')
supplier_df = fetch_redis_data(redis_conn, 'supplier')
customer_df = fetch_redis_data(redis_conn, 'customer')
lineitem_df = fetch_redis_data(redis_conn, 'lineitem')

# Perform renaming to match SQL-like field names
supplier_df = supplier_df.rename(columns={'S_NATIONKEY': 'N_NATIONKEY'})
customer_df = customer_df.rename(columns={'C_NATIONKEY': 'N_NATIONKEY'})

# Merge tables based on conditions
merged_df = supplier_df.merge(nation_df, on='N_NATIONKEY', suffixes=('_SUPPLIER', '_N1'))
merged_df = merged_df.merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
merged_df = merged_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(nation_df, left_on='N_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_SUPPLIER', '_N2'))

# Filter based on criteria and calculate VOLUME
target_nations = ['JAPAN', 'INDIA']
filtered_df = merged_df[
    ((merged_df['N_NAME_SUPPLIER'] == 'JAPAN') & (merged_df['N_NAME_N2'] == 'INDIA')) |
    ((merged_df['N_NAME_SUPPLIER'] == 'INDIA') & (merged_df['N_NAME_N2'] == 'JAPAN'))
]
filtered_df = filtered_df[
    filtered_df['L_SHIPDATE'].between(convert_str_to_date('1995-01-01'), convert_str_to_date('1996-12-31'))
]
filtered_df['L_YEAR'] = filtered_df['L_SHIPDATE'].dt.year
filtered_df['VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by the necessary columns and sum up the VOLUME
result = filtered_df.groupby(['N_NAME_SUPPLIER', 'N_NAME_N2', 'L_YEAR'])['VOLUME'].sum().reset_index()
result = result.rename(columns={
    'N_NAME_SUPPLIER': 'SUPP_NATION',
    'N_NAME_N2': 'CUST_NATION',
    'L_YEAR': 'L_YEAR',
    'VOLUME': 'REVENUE'
})

# Order the results
result = result.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write to CSV
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)
