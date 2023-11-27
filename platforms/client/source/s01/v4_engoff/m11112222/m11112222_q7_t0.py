import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client["tpch"]
nation_collection = mongo_db["nation"]
supplier_collection = mongo_db["supplier"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
customer_df = pd.read_json(redis_client.get('customer'))
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Get data from MongoDB
nation_df = pd.DataFrame(list(nation_collection.find()))
supplier_df = pd.DataFrame(list(supplier_collection.find()))

# Preprocess Mongo data (rename columns to remove prefixes)
nation_df.rename(columns=lambda x: x[2:], inplace=True)
supplier_df.rename(columns=lambda x: x[2:], inplace=True)

# Filter nations for India and Japan
filtered_nations = nation_df[nation_df['NAME'].isin(['INDIA', 'JAPAN'])]

# Filter suppliers from India and Japan
supplier_filtered = supplier_df[supplier_df['NATIONKEY'].isin(filtered_nations['NATIONKEY'])]

# Merge Redis dataframes
merged_df = pd.merge(lineitem_df, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = pd.merge(merged_df, customer_df, left_on='L_CUSTKEY', right_on='C_CUSTKEY')

# Filter for 1995 and 1996
merged_df['L_SHIPDATE'] = pd.to_datetime(merged_df['L_SHIPDATE'])
merged_df = merged_df[merged_df['L_SHIPDATE'].dt.year.isin([1995, 1996])]

# Calculate revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Merge with supplier to get supplier nation
final_df = pd.merge(merged_df, supplier_filtered, left_on='L_SUPPKEY', right_on='SUPPKEY')

# Select only shipments between India and Japan
final_df = final_df[
    (final_df['NATIONKEY_x'] != final_df['NATIONKEY_y']) & 
    (final_df['NATIONKEY_y'].isin(filtered_nations['NATIONKEY'])) & 
    (final_df['NATIONKEY_x'].isin(filtered_nations['NATIONKEY']))
]

# Get supplier nation, customer nation
final_df['SUPPLIER_NATION'] = final_df['NATIONKEY_y'].map(nation_df.set_index('NATIONKEY')['NAME'])
final_df['CUSTOMER_NATION'] = final_df['NATIONKEY_x'].map(nation_df.set_index('NATIONKEY')['NAME'])

# Output columns: Supplier nation, Customer nation, Year, Revenue
output_df = final_df[['SUPPLIER_NATION', 'CUSTOMER_NATION', 'L_SHIPDATE', 'REVENUE']]
output_df['YEAR'] = output_df['L_SHIPDATE'].dt.year
output_df.drop('L_SHIPDATE', axis=1, inplace=True)

# Order by Supplier nation, Customer nation, and Year
output_df = output_df.sort_values(by=['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR'])

# Write result to CSV
output_df.to_csv('query_output.csv', index=False)
