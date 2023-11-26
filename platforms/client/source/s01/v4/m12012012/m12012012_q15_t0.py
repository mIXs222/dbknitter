# query.py
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis
import csv

# Establish a connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
supplier_collection = mongo_db["supplier"]

# Establish a connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Function to convert Redis data to a Pandas DataFrame
def redis_to_dataframe(key):
    data = redis_client.get(key)
    df = pd.DataFrame(data)
    return df

# Fetch data from MongoDB
supplier_df = pd.DataFrame(list(supplier_collection.find({}, {'_id': 0})))

# Fetch data from Redis
lineitem_df = redis_to_dataframe('lineitem')

# Filter lineitem data by shipdate
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] < end_date)]

# Calculate total revenue for each supplier
revenue0_df = filtered_lineitem_df.groupby('L_SUPPKEY').apply(
    lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()).reset_index(name='TOTAL_REVENUE')
revenue0_df.rename(columns={'L_SUPPKEY': 'SUPPLIER_NO'}, inplace=True)

# Merge supplier dataframe with revenue dataframe
merged_df = supplier_df.merge(revenue0_df, left_on='S_SUPPKEY', right_on='SUPPLIER_NO')

# Find the max total revenue
max_total_revenue = merged_df['TOTAL_REVENUE'].max()

# Select suppliers with the max total revenue
result_df = merged_df[merged_df['TOTAL_REVENUE'] == max_total_revenue]

# Sort by S_SUPPKEY
result_df = result_df.sort_values('S_SUPPKEY')

# Select the required columns
output_df = result_df[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Write the output to a CSV file
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
