import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

# Function to get data from MongoDB
def get_mongodb_data(client, db_name, table_name):
    db = client[db_name]
    collection = db[table_name]
    data = list(collection.find())
    return pd.DataFrame(data)

# Function to generate a redis connection
def get_redis_connection(hostname, port, db_name):
    return DirectRedis(host=hostname, port=port, db=db_name, decode_responses=True)

# Function to get data from Redis
def get_redis_data(connection, table_name):
    data = connection.get(table_name)
    return pd.read_json(data, orient='records')

# MongoDB connection
client = pymongo.MongoClient('mongodb', 27017)
# Get nation data
nation_df = get_mongodb_data(client, 'tpch', 'nation')

# Redis connection
redis_conn = get_redis_connection('redis', 6379, 0)
# Get supplier and partsupp data
supplier_df = get_redis_data(redis_conn, 'supplier')
partsupp_df = get_redis_data(redis_conn, 'partsupp')

# Merge data on nation and supplier
merged_df = supplier_df.merge(nation_df, how='left', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
# Filter out German suppliers
german_suppliers = merged_df.loc[merged_df['N_NAME'] == 'GERMANY']

# Merge German suppliers with partsupp
important_stock_df = german_suppliers.merge(partsupp_df, how='inner', on='S_SUPPKEY')
# Compute total value of parts
important_stock_df['TOTAL_VALUE'] = important_stock_df['PS_AVAILQTY'] * important_stock_df['PS_SUPPLYCOST']
# Compute the sum of total value
total_value_sum = important_stock_df['TOTAL_VALUE'].sum()
# Filter the parts with a significant percentage of the total value
important_stock_df = important_stock_df[important_stock_df['TOTAL_VALUE'] > 0.0001 * total_value_sum]
# Sort the parts in descending order of value
important_stock_df = important_stock_df.sort_values(by='TOTAL_VALUE', ascending=False)
# Select the required columns
result_df = important_stock_df[['PS_PARTKEY', 'TOTAL_VALUE']]
# Write to query_output.csv file
result_df.to_csv('query_output.csv', index=False)
