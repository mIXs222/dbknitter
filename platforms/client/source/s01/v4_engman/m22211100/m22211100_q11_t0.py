import pymongo
from bson import json_util
import pandas as pd
from direct_redis import DirectRedis
import csv

# MongoDB connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client["tpch"]
supplier = pd.DataFrame(list(mongo_db.supplier.find(
    {'S_NATIONKEY': {'$exists': True}},
    {'S_SUPPKEY': 1, '_id': 0}
)))
partsupp = pd.DataFrame(list(mongo_db.partsupp.find(
    {}, 
    {'PS_PARTKEY': 1, 'PS_SUPPKEY': 1, 'PS_AVAILQTY': 1, 'PS_SUPPLYCOST': 1, '_id': 0}
)))

# Redis connection
redis_client = DirectRedis(port=6379, host="redis")
nation_json = redis_client.get('nation')
nation_df = pd.read_json(nation_json, orient='records')

# Filter German suppliers
german_nationkeys = nation_df[nation_df['N_NAME'] == 'GERMANY']['N_NATIONKEY'].tolist()
german_suppliers = supplier[supplier['S_NATIONKEY'].isin(german_nationkeys)]

# Join suppliers with partsupp on S_SUPPKEY = PS_SUPPKEY
joined_data = pd.merge(
    german_suppliers,
    partsupp,
    how="inner",
    left_on="S_SUPPKEY",
    right_on="PS_SUPPKEY"
)

# Calculate the value, sum of value, and important stock
joined_data['VALUE'] = joined_data['PS_AVAILQTY'] * joined_data['PS_SUPPLYCOST']
total_value = joined_data['VALUE'].sum()
important_stock_condition = (joined_data['VALUE'] > total_value * 0.0001)
important_stock = joined_data[important_stock_condition]

# Prepare results
results = important_stock[['PS_PARTKEY', 'VALUE']]
results = results.sort_values('VALUE', ascending=False)

# Write results to CSV
results.to_csv('query_output.csv', index=False)
