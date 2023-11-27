from pymongo import MongoClient
from bson.objectid import ObjectId
import pandas as pd
from datetime import datetime
import direct_redis

# Establish a connection to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']
part_collection = mongodb['part']

# Establish a connection to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Define the date range for the query as provided
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)

# Fetch data from MongoDB
part_df = pd.DataFrame(list(part_collection.find({'P_NAME': {'$regex': '.*Promo.*'}})))

# Fetch data from Redis
lineitem_df = r.get('lineitem')
lineitem_df = pd.read_json(lineitem_df)

# Filter records between start and end dates (after including necessary columns from Redis, assuming the SHIPDATE is in a suitable format)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] < end_date)]

# Join the data on L_PARTKEY == P_PARTKEY (assuming 'L_PARTKEY' and 'P_PARTKEY' columns are present in the respective tables)
merged_df = filtered_lineitem.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate revenue (ensure that the column names are correctly capitalized as they appear in your data)
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Calculate total revenue
total_revenue = merged_df['REVENUE'].sum()

# Determine the count of promotional parts
promo_revenue = merged_df.loc[merged_df['P_NAME'].str.contains('Promo'), 'REVENUE'].sum()

# Calculate the percentage of the revenue that was derived from promotional parts
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue > 0 else 0

# Output the result to a CSV file
pd.DataFrame({'Promotion_Revenue_Percentage': [promo_revenue_percentage]}).to_csv('query_output.csv', index=False)
