# python_code.py
import pymongo
import direct_redis
import pandas as pd
from datetime import datetime

# MongoDB Connection
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]
supplier_collection = mongodb_db["supplier"]
suppliers_df = pd.DataFrame(list(supplier_collection.find(
    {}, {"_id": 0, "S_SUPPKEY": 1, "S_NAME": 1, "S_ADDRESS": 1, "S_PHONE": 1}
)))

# Redis connection using direct_redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
# It is assumed that the data structure in Redis for 'lineitem' is serialized DataFrame
lineitem_df_serialized = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_df_serialized)

# Filter lineitem dataframe for the date range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)
]

# Calculate revenue
filtered_lineitem_df['REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (
    1 - filtered_lineitem_df['L_DISCOUNT']
)

# Calculate total revenue per supplier
supplier_revenue = filtered_lineitem_df.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()
supplier_revenue.rename(columns={'L_SUPPKEY': 'S_SUPPKEY', 'REVENUE': 'TOTAL_REVENUE'}, inplace=True)

# Merge with suppliers
result_df = pd.merge(suppliers_df, supplier_revenue, on='S_SUPPKEY')

# Find the top supplier revenue
max_revenue = result_df['TOTAL_REVENUE'].max()
top_suppliers_df = result_df[result_df['TOTAL_REVENUE'] == max_revenue]

# Sort by supplier number (S_SUPPKEY)
top_suppliers_df.sort_values('S_SUPPKEY', inplace=True)

# Write to CSV
top_suppliers_df.to_csv('query_output.csv', index=False)
