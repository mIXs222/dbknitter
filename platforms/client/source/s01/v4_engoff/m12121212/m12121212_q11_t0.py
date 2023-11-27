import pymongo
from pymongo import MongoClient
import pandas as pd
import redis
import direct_redis

# Connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
nation_collection = db['nation']
partsupp_collection = db['partsupp']

# Query MongoDB for nations with the name "GERMANY"
nation_germany = list(nation_collection.find({'N_NAME': 'GERMANY'}, {'N_NATIONKEY': 1}))

# Extract NATIONKEY for "GERMANY"
nationkey_germany = nation_germany[0]['N_NATIONKEY'] if nation_germany else None

# Connection to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch the supplier DataFrame from Redis
suppliers_df = pd.read_json(r.get('supplier'), orient='records')

# Filter for suppliers from GERMANY
suppliers_germany_df = suppliers_df[suppliers_df['S_NATIONKEY'] == nationkey_germany]

# Query MongoDB for partsupp data
partsupp_df = pd.DataFrame(list(partsupp_collection.find()))

# Merge suppliers and partsupp dataframes to get parts supplied by german suppliers
german_partsupp_df = pd.merge(suppliers_germany_df, partsupp_df, how='inner', left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Calculate total value (PS_SUPPLYCOST * PS_AVAILQTY) for parts
german_partsupp_df['VALUE'] = german_partsupp_df['PS_SUPPLYCOST'] * german_partsupp_df['PS_AVAILQTY']

# Calculate total value of all parts
total_value = german_partsupp_df['VALUE'].sum()

# Filter parts that represent a significant percentage of the total value
important_parts_df = german_partsupp_df[german_partsupp_df['VALUE'] > total_value * 0.0001]

# Select relevant columns and sort by value in descending order
important_parts_df = important_parts_df[['PS_PARTKEY', 'VALUE']].sort_values(by='VALUE', ascending=False)

# Write the result to a CSV file
important_parts_df.to_csv('query_output.csv', index=False)
