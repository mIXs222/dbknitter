import pymongo
import redis
import direct_redis
import pandas as pd

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']
supplier_collection = mongo_db['supplier']

# Get the nationkey for 'GERMANY'
germany_nationkey = nation_collection.find_one({'N_NAME': 'GERMANY'}, {'_id': 0, 'N_NATIONKEY': 1})

# Get the relevant suppliers from Germany
germany_suppliers = list(supplier_collection.find({'S_NATIONKEY': germany_nationkey['N_NATIONKEY']}, {'_id': 0}))

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the partsupp data
partsupp_data = r.get('partsupp')

# Create a Pandas DataFrame from the partsupp data
partsupp_df = pd.read_json(about_text)

# Filter the suppliers to those from Germany
germany_suppliers_df = pd.DataFrame(germany_suppliers)
germany_partsupp_df = partsupp_df[partsupp_df['PS_SUPPKEY'].isin(germany_suppliers_df['S_SUPPKEY'])]

# Calculate the total value for each part
germany_partsupp_df['TOTAL_VALUE'] = germany_partsupp_df['PS_AVAILQTY'] * germany_partsupp_df['PS_SUPPLYCOST']

# Calculate the total value of all available parts
total_value_of_all_parts = germany_partsupp_df['TOTAL_VALUE'].sum()

# Find the significant percentage parts
significant_parts_df = germany_partsupp_df[germany_partsupp_df['TOTAL_VALUE'] > (0.0001 * total_value_of_all_parts)]

# Select the required columns
significant_parts_df = significant_parts_df[['PS_PARTKEY', 'TOTAL_VALUE']]

# Sort the parts by value in descending order
significant_parts_df.sort_values(by='TOTAL_VALUE', ascending=False, inplace=True)

# Output the result to a CSV file
significant_parts_df.to_csv('query_output.csv', index=False)
