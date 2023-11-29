# File name: find_important_stock.py

import pymongo
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
nation_collection = mongo_db["nation"]
partsupp_collection = mongo_db["partsupp"]

# Connect to Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Load 'supplier' table into DataFrame using Redis
supplier_data = eval(redis_client.get('supplier'))
supplier_df = pd.DataFrame(supplier_data)

# Load 'nation' and 'partsupp' tables into DataFrames using MongoDB
nation_df = pd.DataFrame(list(nation_collection.find()))
partsupp_df = pd.DataFrame(list(partsupp_collection.find()))

# Find suppliers from GERMANY
germany_nationkey = nation_df[nation_df['N_NAME'] == 'GERMANY']['N_NATIONKEY'].values[0]
germany_supplier_df = supplier_df[supplier_df['S_NATIONKEY'] == germany_nationkey]

# Join the suppliers with partsupp on supplier key
important_stock_df = germany_supplier_df.merge(partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Calculate the value of the stock and filter
important_stock_df['STOCK_VALUE'] = important_stock_df['PS_AVAILQTY'] * important_stock_df['PS_SUPPLYCOST']
total_value = important_stock_df['STOCK_VALUE'].sum()
significant_stock_df = important_stock_df[important_stock_df['STOCK_VALUE'] > total_value * 0.0001]

# Sort by stock value in descending order and select relevant columns
significant_stock_df.sort_values(by='STOCK_VALUE', ascending=False, inplace=True)
output_df = significant_stock_df[['PS_PARTKEY', 'STOCK_VALUE']]

# Output the result to CSV
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
