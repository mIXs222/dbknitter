# import necessary packages
from pymongo import MongoClient
import pandas as pd
import direct_redis
import datetime

# connect to mongodb
client = MongoClient('mongodb', 27017)
db = client.tpch
lineitem_collection = db.lineitem

# query lineitem data in mongodb
lineitem_query = {
    "L_RECEIPTDATE": {"$gte": datetime.datetime(1994, 1, 1), "$lt": datetime.datetime(1995, 1, 1)},
    "L_SHIPMODE": {"$in": ["MAIL", "SHIP"]},
    "L_COMMITDATE": {"$lt": "$L_COMMITDATE"}
}
lineitem_cursor = lineitem_collection.find(lineitem_query, projection={'_id': False})
lineitem_df = pd.DataFrame(list(lineitem_cursor))

# Connect to redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get orders dataframe from redis
orders_df = pd.read_json(redis_client.get('orders'))

# Merging the data from both sources based on L_ORDERKEY and O_ORDERKEY
merged_df = pd.merge(left=lineitem_df, right=orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Check if L_RECEIPTDATE exceeds L_COMMITDATE, and shipping before L_COMMITDATE
merged_df['is_late'] = merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE']
merged_df = merged_df[merged_df['is_late'] & (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE'])]

# Add column for priority grouping
merged_df['priority_group'] = merged_df['O_ORDERPRIORITY'].apply(lambda x: 'HIGH' if x in ['URGENT', 'HIGH'] else 'LOW')

# Grouping by L_SHIPMODE and priority_group, and counting
grouped_df = merged_df.groupby(['L_SHIPMODE', 'priority_group']).size().reset_index(name='count')

# Pivoting the dataframe to get required format
pivot_df = grouped_df.pivot(index='L_SHIPMODE', columns='priority_group', values='count').fillna(0)

# Save result to csv
pivot_df.to_csv('query_output.csv')

print("Query output saved to query_output.csv")
