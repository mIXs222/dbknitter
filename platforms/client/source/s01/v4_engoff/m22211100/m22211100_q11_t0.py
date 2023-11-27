# query.py

from pymongo import MongoClient
from direct_redis import DirectRedis
import pandas as pd

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Fetch data from MongoDB collections
supplier_df = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0})))
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find({}, {'_id': 0})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from Redis and save it into pandas DataFrame
nation_df = pd.read_msgpack(redis_client.get('nation'))

# Filter suppliers from GERMANY
germany_suppliers = nation_df[nation_df['N_NAME'] == 'GERMANY']['N_NATIONKEY']
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(germany_suppliers)]

# Join supplier_df and partsupp_df on S_SUPPKEY and PS_SUPPKEY
joined_df = supplier_df.merge(partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Calculate the total value of available parts as supply cost multiplied by available quantity
joined_df['TOTAL_VALUE'] = joined_df['PS_SUPPLYCOST'] * joined_df['PS_AVAILQTY']

# Calculate the sum of the total value of all parts
total_value_sum = joined_df['TOTAL_VALUE'].sum()

# Find parts that represent a significant percentage of the total available parts
important_parts_df = joined_df[joined_df['TOTAL_VALUE'] / total_value_sum > 0.0001]

# Select the required columns
important_parts_output_df = important_parts_df[['PS_PARTKEY', 'TOTAL_VALUE']]

# Sort the parts by value in descending order
important_parts_output_df.sort_values(by='TOTAL_VALUE', ascending=False, inplace=True)

# Write output to query_output.csv
important_parts_output_df.to_csv('query_output.csv', index=False)
