import pymongo
from pymongo import MongoClient
import pandas as pd
import datetime
from direct_redis import DirectRedis

# MongoDB connection setup
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_lineitem_collection = mongo_db['lineitem']

# Redis connection setup
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get orders data from Redis into a Pandas DataFrame
redis_orders = redis_client.get('orders')
orders_df = pd.read_json(redis_orders)

# Convert the order date in orders DataFrame to datetime
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filter orders with specified dates and high/low priority
date_filter = (orders_df['O_ORDERDATE'] >= datetime.datetime(1994, 1, 1)) & \
              (orders_df['O_ORDERDATE'] <= datetime.datetime(1994, 12, 31))
priority_high_filter = orders_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])
priority_low_filter = ~orders_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])
orders_high_priority = orders_df[date_filter & priority_high_filter]
orders_low_priority = orders_df[date_filter & priority_low_filter]

# Fetch lineitem data from MongoDB
mongo_lineitem_query = {
    'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
    'L_COMMITDATE': {'$lt': "$L_RECEIPTDATE"},
    'L_SHIPDATE': {'$lt': "$L_COMMITDATE"},
    'L_RECEIPTDATE': {
        '$gte': datetime.datetime(1994, 1, 1),
        '$lte': datetime.datetime(1994, 12, 31)
    }
}
lineitem_df = pd.DataFrame(list(mongo_lineitem_collection.find(mongo_lineitem_query)))

# Merge and aggregate data
merged_high = pd.merge(lineitem_df, orders_high_priority, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
merged_low = pd.merge(lineitem_df, orders_low_priority, left_on="L_ORDERKEY", right_on="O_ORDERKEY")

high_count = merged_high.groupby('L_SHIPMODE').size().reset_index(name='HIGH_LINE_COUNT')
low_count = merged_low.groupby('L_SHIPMODE').size().reset_index(name='LOW_LINE_COUNT')

# Join the high priority and low priority counts
result = pd.merge(high_count, low_count, on='L_SHIPMODE', how='outer').fillna(0)
result = result.sort_values('L_SHIPMODE')

# Output results to a csv file
result.to_csv('query_output.csv', index=False)

# Cleaning up the connection
mongo_client.close()
redis_client.close()
