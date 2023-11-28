from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import direct_redis
import csv

# Function to connect to MongoDB and retrieve the data
def get_mongo_data(collection_name):
    client = MongoClient('mongodb', 27017)
    db = client['tpch']
    collection = db[collection_name]
    data = list(collection.find())
    return pd.DataFrame(data)

# Function to connect to Redis and retrieve the data
def get_redis_data(table_name):
    redis_client = direct_redis.DirectRedis(host='redis', port=6379)
    data_str = redis_client.get(table_name).decode('utf-8')
    data = pd.read_json(data_str, lines=True)
    return data

# Get MongoDB data
supplier_data = get_mongo_data('supplier')

# Get Redis data
lineitem_data = get_redis_data('lineitem')

# Convert string date to datetime for comparison
lineitem_data['L_SHIPDATE'] = pd.to_datetime(lineitem_data['L_SHIPDATE'])

# Establish the time period for the query
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)

# Filter line items within the specified time period
filtered_lineitems = lineitem_data[(lineitem_data['L_SHIPDATE'] >= start_date) & (lineitem_data['L_SHIPDATE'] <= end_date)]

# Calculate the total revenue
filtered_lineitems['TOTAL_REVENUE'] = filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])

# Group by supplier number and calculate total revenue
revenue_per_supplier = filtered_lineitems.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Rename columns to match original SQL query expectations
revenue_per_supplier.rename(columns={'L_SUPPKEY': 'SUPPLIER_NO', 'TOTAL_REVENUE': 'TOTAL_REVENUE'}, inplace=True)

# Merge the supplier data with the total revenue data
merged_data = pd.merge(supplier_data, revenue_per_supplier, left_on='S_SUPPKEY', right_on='SUPPLIER_NO')

# Find the supplier with maximum total revenue
max_revenue_supplier = merged_data[merged_data['TOTAL_REVENUE'] == merged_data['TOTAL_REVENUE'].max()]

# Sort by supplier key (S_SUPPKEY)
sorted_data = max_revenue_supplier.sort_values(by='S_SUPPKEY')

# Select relevant columns
final_data = sorted_data[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Write the query output to a CSV file
final_data.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
