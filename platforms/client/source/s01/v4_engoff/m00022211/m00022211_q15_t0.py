# File: query.py
from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Redis connection
redis = DirectRedis(host='redis', port=6379, db=0)

# Load data from Redis into a DataFrame
supplier_data = redis.get('supplier')
supplier_df = pd.read_json(supplier_data)

# Load data from MongoDB into a DataFrame
query_mongo = {
    'L_SHIPDATE': {'$gte': '1996-01-01', '$lt': '1996-04-01'}
}
projection_mongo = {
    '_id': False,
    'L_SUPPKEY': True,
    'L_EXTENDEDPRICE': True,
    'L_DISCOUNT': True
}
lineitem_df = pd.DataFrame(list(lineitem_collection.find(query_mongo, projection_mongo)))

# Calculate total revenue contribution for each supplier
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
revenue_per_supplier = lineitem_df.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()

# Find the top supplier(s)
max_revenue = revenue_per_supplier['REVENUE'].max()
top_suppliers = revenue_per_supplier[revenue_per_supplier['REVENUE'] == max_revenue]

# Merge with supplier information
top_suppliers = top_suppliers.merge(supplier_df, how='left', left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Select and order the columns
ordered_columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
top_suppliers = top_suppliers[ordered_columns].sort_values(by='S_SUPPKEY')

# Save to CSV
top_suppliers.to_csv('query_output.csv', index=False)
