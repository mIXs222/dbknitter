# query.py
from pymongo import MongoClient
import pandas as pd
import direct_redis
import datetime

# MongoDB connection and data retrieval
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']
part_df = pd.DataFrame(list(part_collection.find(
    {"P_TYPE": {"$regex": "^PROMO"}},
    {"P_PARTKEY": 1, "_id": 0}
)))

# Redis connection and data retrieval
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
lineitem_df = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_df)

# Convert dates to datetime
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter lineitem data based on date
start_date = datetime.datetime(1995, 9, 1)
end_date = datetime.datetime(1995, 10, 1)
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] < end_date)]

# Merging part and lineitem dataframes on L_PARTKEY and P_PARTKEY
merged_df = pd.merge(filtered_lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the promotion revenue
merged_df['ADJUSTED_PRICE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
promo_revenue = (merged_df['ADJUSTED_PRICE'].sum()) * 100.0

# Calculate total revenue
total_revenue = merged_df['ADJUSTED_PRICE'].sum() if not merged_df.empty else 1

# Calculate PROMO_REVENUE percentage
promo_revenue_percentage = promo_revenue / total_revenue

# Output result to a CSV
result_df = pd.DataFrame([{'PROMO_REVENUE': promo_revenue_percentage}])
result_df.to_csv('query_output.csv', index=False)
