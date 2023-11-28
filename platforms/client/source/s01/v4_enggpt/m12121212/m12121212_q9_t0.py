import pymongo
import redis
import pandas as pd
from direct_redis import DirectRedis
import re
from datetime import datetime

# Function to connect to MongoDB
def get_mongo_data():
    mongo_client = pymongo.MongoClient("mongodb", 27017)
    db = mongo_client['tpch']
    
    # Dictionary to hold dataframes
    mongo_dfs = {}
    
    # Query and create DataFrames for each table
    for collection_name in ['nation', 'part', 'partsupp', 'orders']:
        collection = db[collection_name]
        mongo_dfs[collection_name] = pd.DataFrame(list(collection.find()))
        
    return mongo_dfs

# Function to connect to Redis and fetch data
def get_redis_data():
    r = DirectRedis(host='redis', port=6379, db=0)
    
    # Dictionary to hold dataframes
    redis_dfs = {}
    
    # Fetch and create DataFrames for each key in Redis
    for key in ['supplier', 'lineitem']:
        redis_dfs[key] = pd.read_json(r.get(key))
    
    return redis_dfs

# Get data from MongoDB
mongo_data = get_mongo_data()

# Get data from Redis
redis_data = get_redis_data()

# Filter parts to include only those containing 'dim' in their name
parts_with_dim = mongo_data['part'][mongo_data['part']['P_NAME'].str.contains('dim', flags=re.IGNORECASE)]

# Perform join operations
parts_nations = parts_with_dim.merge(mongo_data['partsupp'], left_on='P_PARTKEY', right_on='PS_PARTKEY')
lin_joined = redis_data['lineitem'].merge(parts_nations, left_on='L_PARTKEY', right_on='P_PARTKEY')
order_nation = mongo_data['orders'].merge(mongo_data['nation'], left_on='O_CUSTKEY', right_on='N_NATIONKEY')
lin_joined_ok = lin_joined.merge(order_nation, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
lin_supplier = lin_joined_ok.merge(redis_data['supplier'], left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Calculating profit
lin_supplier['PROFIT'] = (lin_supplier['L_EXTENDEDPRICE'] * (1 - lin_supplier['L_DISCOUNT'])) - (lin_supplier['PS_SUPPLYCOST'] * lin_supplier['L_QUANTITY'])

# Group by nation and year, and calculate total profit
grouped = lin_supplier.groupby([lin_supplier['N_NAME'], lin_supplier['O_ORDERDATE'].dt.year])['PROFIT'].sum().reset_index()

# Rename columns
grouped.rename(columns={'N_NAME': 'NATION', 'O_ORDERDATE': 'YEAR', 'PROFIT': 'TOTAL_PROFIT'}, inplace=True)

# Sort results
sorted_grouped = grouped.sort_values(by=['NATION', 'YEAR'], ascending=[True, False])

# Write to CSV
sorted_grouped.to_csv('query_output.csv', index=False)
