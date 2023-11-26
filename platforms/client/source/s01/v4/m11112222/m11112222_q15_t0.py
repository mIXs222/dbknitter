# Python code to execute the equivalent of the SQL query on MongoDB and Redis

import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
supplier_collection = mongo_db["supplier"]

# Get suppliers data (projection to include only relevant fields)
suppliers_data = list(supplier_collection.find({}, {
    'S_SUPPKEY': 1,
    'S_NAME': 1,
    'S_ADDRESS': 1,
    'S_PHONE': 1,
    '_id': 0
}))

# Convert suppliers data to DataFrame
suppliers_df = pd.DataFrame(suppliers_data)

# Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Get lineitems data
lineitems_df = pd.DataFrame(redis.get('lineitem'))

# Convert the relevant fields to the appropriate dtype for filtering
lineitems_df['L_SHIPDATE'] = pd.to_datetime(lineitems_df['L_SHIPDATE'])
lineitems_df['L_EXTENDEDPRICE'] = pd.to_numeric(lineitems_df['L_EXTENDEDPRICE'])
lineitems_df['L_DISCOUNT'] = pd.to_numeric(lineitems_df['L_DISCOUNT'])

# Calculate revenue and filter by date
mask = (lineitems_df['L_SHIPDATE'] >= '1996-01-01') & (lineitems_df['L_SHIPDATE'] < '1996-04-01')
revenue_df = lineitems_df.loc[mask].groupby('L_SUPPKEY').apply(
    lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()
).reset_index(name='TOTAL_REVENUE')

# Find max revenue
max_revenue = revenue_df['TOTAL_REVENUE'].max()

# Join data to find suppliers with max revenue
result_df = suppliers_df.merge(revenue_df, how='inner', left_on='S_SUPPKEY', right_on='L_SUPPKEY')
result_df = result_df[result_df['TOTAL_REVENUE'] == max_revenue]

# Select required columns and order by S_SUPPKEY
result_df = result_df[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']].sort_values(by='S_SUPPKEY')

# Write result to a CSV file
result_df.to_csv('query_output.csv', index=False)
