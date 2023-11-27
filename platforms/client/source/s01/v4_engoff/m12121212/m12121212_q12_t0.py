import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis
import csv

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
orders_collection = mongo_db["orders"]

# Retrieve orders with specified O_ORDERPRIORITY and within date range
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)
priority_conditions = {'$in': ['URGENT', 'HIGH']}
orders_query = {
    'O_ORDERDATE': {'$gte': start_date, '$lt': end_date},
    'O_ORDERPRIORITY': priority_conditions
}
orders_projection = {
    '_id': False,
    'O_ORDERKEY': True,
    'O_ORDERPRIORITY': True
}
orders_df = pd.DataFrame(list(orders_collection.find(orders_query, orders_projection)))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve lineitems from Redis and convert to Pandas DataFrame
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Pre-process lineitem data
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Merge dataframes
merged_df = pd.merge(orders_df, lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter according to query conditions and count late items per ship mode and priority
filtered_df = merged_df[(merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE']) &
                        (merged_df['L_SHIPDATE'] <= merged_df['L_COMMITDATE']) &
                        ((merged_df['L_SHIPMODE'] == 'MAIL') | (merged_df['L_SHIPMODE'] == 'SHIP'))]

# Perform counting
results = filtered_df.groupby(['L_SHIPMODE', 'O_ORDERPRIORITY']).size().reset_index(name='COUNT')

# Write output to file
results.to_csv('query_output.csv', index=False)
