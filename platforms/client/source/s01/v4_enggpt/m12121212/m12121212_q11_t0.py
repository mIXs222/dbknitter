# analysis.py
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Define a function to connect to MongoDB and get the table data
def get_mongodb_table_data(collection_name, host='mongodb', port=27017, db_name='tpch'):
    client = pymongo.MongoClient(host, port)
    db = client[db_name]
    collection = db[collection_name]
    data = pd.DataFrame(list(collection.find()))
    return data

# Define a function to connect to Redis and get the table data
def get_redis_table_data(key_name, host='redis', port=6379, db_name=0):
    redis_client = DirectRedis(host=host, port=port, db=db_name)
    data = pd.read_json(redis_client.get(key_name))
    return data

# Get data from MongoDB
nation_data = get_mongodb_table_data('nation')
partsupp_data = get_mongodb_table_data('partsupp')

# Get data from Redis
supplier_data = get_redis_table_data('supplier')

# Merge data to perform analysis
# Filter nations for Germany
germany_nations = nation_data[nation_data['N_NAME'] == 'GERMANY']

# Join supplier with Germany nations to get German suppliers
german_suppliers = supplier_data[supplier_data['S_NATIONKEY'].isin(germany_nations['N_NATIONKEY'])]

# Join partsupp with German suppliers
parts_german_suppliers = partsupp_data.merge(german_suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculate total value for each part (supply cost * available quantity)
parts_german_suppliers['TOTAL_VALUE'] = parts_german_suppliers['PS_SUPPLYCOST'] * parts_german_suppliers['PS_AVAILQTY']

# Group by part key and aggregate the values
grouped_parts = parts_german_suppliers.groupby('PS_PARTKEY').agg({'TOTAL_VALUE': 'sum'}).reset_index()

# Calculate the overall value for Germany-based suppliers as the threshold
total_value_threshold = parts_german_suppliers['TOTAL_VALUE'].sum() * (certain_percentage / 100)
filtered_parts = grouped_parts[grouped_parts['TOTAL_VALUE'] > total_value_threshold]

# Order by calculated value in descending order
final_result = filtered_parts.sort_values('TOTAL_VALUE', ascending=False)

# Write final results to CSV
final_result.to_csv('query_output.csv', index=False)
