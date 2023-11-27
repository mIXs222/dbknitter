from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
supplier_collection = db['supplier']
supplier_df = pd.DataFrame(list(supplier_collection.find({}, {
    '_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1, 'S_PHONE': 1
})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Filter dates
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] < end_date)]

# Calculate total revenue
lineitem_df['TOTAL_REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
revenue0 = lineitem_df.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Merge with suppliers and calculate max revenue
merged_df = pd.merge(supplier_df, revenue0, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
max_revenue = merged_df['TOTAL_REVENUE'].max()
highest_revenue_suppliers = merged_df[merged_df['TOTAL_REVENUE'] == max_revenue]

# Select necessary columns
result = highest_revenue_suppliers[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Sort by supplier key and save to CSV
result.sort_values(by=['S_SUPPKEY'], inplace=True)
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)
