from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Mongo connection
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_supp_coll = mongo_db["supplier"]

# Redis connection
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Date range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)

# Get supplier data from MongoDB
suppliers = pd.DataFrame(list(mongo_supp_coll.find({}, {
    "_id": 0,
    "S_SUPPKEY": 1,
    "S_NAME": 1,
    "S_ADDRESS": 1,
    "S_PHONE": 1
})))

# Get lineitem data from Redis and convert it to a Pandas DataFrame
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Convert dates from string to datetime
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filtering lineitem data within the date range
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)]

# Calculating revenue for each lineitem
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Group by supplier and sum the revenues
revenue_agg = lineitem_df.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()
revenue_agg = revenue_agg.rename(columns={"L_SUPPKEY": "S_SUPPKEY", "REVENUE": "TOTAL_REVENUE"})

# Merge supplier data with aggregated revenue data
merged_df = pd.merge(suppliers, revenue_agg, on="S_SUPPKEY", how="inner")

# Find the supplier with the maximum total revenue
max_revenue_supp = merged_df[merged_df['TOTAL_REVENUE'] == merged_df['TOTAL_REVENUE'].max()]

# Sorting the final DataFrame by S_SUPPKEY in ascending order
final_df = max_revenue_supp.sort_values('S_SUPPKEY')

# Write the result to a CSV file
final_df.to_csv('query_output.csv', index=False)
