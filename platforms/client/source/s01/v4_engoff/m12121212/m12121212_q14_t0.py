import pymongo
from bson.codec_options import CodecOptions
import pandas as pd
import direct_redis
import csv

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
parts_collection = mongo_db['part']

# Retrieve all documents from the 'part' collection and convert to DataFrame
parts_data = list(parts_collection.find({}, projection={'_id': False}))
parts_df = pd.DataFrame(parts_data)

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
lineitem_data = redis_client.get('lineitem')
lineitem_df = pd.DataFrame(eval(lineitem_data))

# Convert date strings to datetime objects for comparison
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Define date range for the query
start_date = pd.Timestamp('1995-09-01')
end_date = pd.Timestamp('1995-10-01')

# Filter the lineitems based on date range and promotional parts
filtered_lineitems_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) &
    (lineitem_df['L_SHIPDATE'] <= end_date) &
    (lineitem_df['L_PARTKEY'].isin(parts_df['P_PARTKEY']))
]

# Calculate revenue
filtered_lineitems_df['REVENUE'] = filtered_lineitems_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitems_df['L_DISCOUNT'])

# Get sum of revenue for filtered data
total_revenue_from_promo = filtered_lineitems_df['REVENUE'].sum()

# Get total revenue from all parts shipped in date range
total_revenue = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) &
    (lineitem_df['L_SHIPDATE'] <= end_date)
]['L_EXTENDEDPRICE'].sum()

# Calculate percentage revenue from promo
percentage_revenue_from_promo = (total_revenue_from_promo / total_revenue) * 100 if total_revenue else 0

# Create result dataframe
result_df = pd.DataFrame([{"Percentage Revenue from Promo": percentage_revenue_from_promo}])

# Write output to CSV
result_df.to_csv('query_output.csv', index=False)
