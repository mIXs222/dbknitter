import pandas as pd
from pymongo import MongoClient
import direct_redis
import csv

# Establish connection to the MongoDB server
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Fetch the nation and supplier data from MongoDB
nation_data = pd.DataFrame(list(mongo_db.nation.find({}, {'_id': 0})))
supplier_data = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0})))

# Filter nations by GERMANY and join supplier data
german_nations = nation_data[nation_data['N_NAME'] == 'GERMANY']
german_suppliers = pd.merge(supplier_data, german_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Establish connection to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, decode_responses=True)
partsupp_data_str = r.get('partsupp')

# Convert partsupp_data from string to DataFrame
partsupp_data = pd.DataFrame(eval(partsupp_data_str))

# Join the partsupp and supplier data
result = pd.merge(german_suppliers, partsupp_data, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Calculate the value of parts and filter significant stock
result['PART_VALUE'] = result['PS_AVAILQTY'] * result['PS_SUPPLYCOST']
total_value = result['PART_VALUE'].sum()
significant_parts = result[result['PART_VALUE'] > 0.0001 * total_value]

# Select and sort the significant parts
output = significant_parts[['PS_PARTKEY', 'PART_VALUE']].sort_values(by='PART_VALUE', ascending=False)

# Write the result to a CSV file
output.to_csv('query_output.csv', index=False)
