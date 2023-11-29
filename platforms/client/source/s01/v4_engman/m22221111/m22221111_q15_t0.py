# Python code to execute the top supplier query across different data platforms
import pymongo
import pandas as pd
import numpy as np
import redis
import direct_redis

# connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Query for the lineitem data in MongoDB
query = {
    'L_SHIPDATE': {
        '$gte': '1996-01-01',
        '$lt': '1996-04-01'
    }
}
projection = {
    '_id': 0,
    'L_SUPPKEY': 1,
    'L_EXTENDEDPRICE': 1,
    'L_DISCOUNT': 1
}
lineitem_df = pd.DataFrame(list(lineitem_collection.find(query, projection)))

# Calculate the revenue
lineitem_df['TOTAL_REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
supplier_revenue = lineitem_df.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the supplier data from Redis
supplier_df = pd.read_json(redis_client.get('supplier'))

# Merge with the lineitem data to get the top supplier
result_df = pd.merge(supplier_df, supplier_revenue, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
result_df = result_df.sort_values('TOTAL_REVENUE', ascending=False)

# Get top total revenue value
top_total_revenue = result_df['TOTAL_REVENUE'].max()

# Filter the result to only include suppliers with top total revenue
top_suppliers = result_df[result_df['TOTAL_REVENUE'] == top_total_revenue]
top_suppliers = top_suppliers[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]
top_suppliers.sort_values('S_SUPPKEY', inplace=True)

# Write the final result to a .csv file
top_suppliers.to_csv('query_output.csv', index=False)
