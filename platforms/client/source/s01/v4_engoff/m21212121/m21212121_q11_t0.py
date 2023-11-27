import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Constants
GREAT_VALUE_PERCENTAGE = 0.0001
GERMANY = 'GERMANY'

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MongoDB
supplier_df = pd.DataFrame(list(mongo_db.supplier.find({'S_NATIONKEY': 1})))  # Assuming nationkey for GERMANY is 1

# Retrieve data from Redis
nation_df = pd.read_json(redis_client.get('nation').decode('utf-8'))
partsupp_df = pd.read_json(redis_client.get('partsupp').decode('utf-8'))

# Join the tables on supplier key
merged_df = pd.merge(left=partsupp_df, right=supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Filter out non-German suppliers
german_nations = nation_df[nation_df['N_NAME'] == GERMANY]['N_NATIONKEY']
german_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(german_nations)]
german_partsupp = partsupp_df[partsupp_df['PS_SUPPKEY'].isin(german_suppliers['S_SUPPKEY'])]

# Calculate the total value of all available parts
german_partsupp['TOTAL_VALUE'] = german_partsupp['PS_AVAILQTY'] * german_partsupp['PS_SUPPLYCOST']
total_value = german_partsupp['TOTAL_VALUE'].sum()

# Find parts that represent a significant percentage of the total value
important_parts = german_partsupp[german_partsupp['TOTAL_VALUE'] > total_value * GREAT_VALUE_PERCENTAGE]
important_parts = important_parts[['PS_PARTKEY', 'TOTAL_VALUE']]

# Sort by descending order of value
important_parts = important_parts.sort_values(by='TOTAL_VALUE', ascending=False)

# Write the query's output to CSV
important_parts.to_csv('query_output.csv', index=False)
