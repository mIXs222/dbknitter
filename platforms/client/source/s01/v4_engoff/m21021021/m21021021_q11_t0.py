# Import necessary libraries
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
parts_table = mongo_db["partsupp"]

# Fetch partsupp data from MongoDB
partsupp_df = pd.DataFrame(list(parts_table.find({}, {'_id': 0})))

# Redis connection setup
redis_client = DirectRedis(host='redis', port=6379, db=0)
 
# Fetch nation and supplier data from Redis and convert to DataFrame
nation_df = pd.read_json(redis_client.get('nation'))
supplier_df = pd.read_json(redis_client.get('supplier'))

# Merge the DataFrames to process the SQL-like query
merged_df = (
    partsupp_df
    .merge(supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
)

# Filter for suppliers in GERMANY
germany_suppliers = merged_df[merged_df['N_NAME'] == 'GERMANY']

# Calculate the total value of the stock
germany_suppliers['TOTAL_VALUE'] = germany_suppliers['PS_AVAILQTY'] * germany_suppliers['PS_SUPPLYCOST']

# Consider only significant stock
significant_stock = germany_suppliers[germany_suppliers['TOTAL_VALUE'] > germany_suppliers['TOTAL_VALUE'].sum() * 0.0001]

# Select relevant columns and sort
result = significant_stock[['PS_PARTKEY', 'TOTAL_VALUE']]
result_sorted = result.sort_values('TOTAL_VALUE', ascending=False)

# Write to CSV
result_sorted.to_csv('query_output.csv', index=False)
