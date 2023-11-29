# code.py
import pandas as pd
import pymongo
from direct_redis import DirectRedis

# Connection information for MongoDB
mongo_host = 'mongodb'
mongo_port = 27017
mongo_db_name = 'tpch'

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host=mongo_host, port=mongo_port)
mongo_db = mongo_client[mongo_db_name]

# Connection information for Redis
redis_host = 'redis'
redis_port = 6379
redis_db_name = 0

# Connect to Redis
redis_client = DirectRedis(host=redis_host, port=redis_port, db=redis_db_name)

# Retrieve orders from MongoDB within the date range and convert to Pandas DataFrame
orders_collection = mongo_db['orders']
orders_query = {
    'O_ORDERDATE': {
        '$gte': '1993-07-01',
        '$lt': '1993-10-01'
    }
}
orders_data = list(orders_collection.find(orders_query))
orders_df = pd.DataFrame(orders_data)

# Retrieve lineitems from Redis and convert to Pandas DataFrame
lineitems = pd.read_json(redis_client.get('lineitem'))

# Filter lineitems where receipt date is later than commit date
late_lineitems_df = lineitems[lineitems['L_RECEIPTDATE'] > lineitems['L_COMMITDATE']]

# Merge orders and late lineitems on orderkey
merged_df = orders_df.merge(late_lineitems_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner').drop_duplicates(subset=['O_ORDERKEY'])

# Group by O_ORDERPRIORITY and count unique orders
order_priority_count = merged_df.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].nunique().reset_index(name='ORDER_COUNT')

# Order by O_ORDERPRIORITY ascending
order_priority_count.sort_values('O_ORDERPRIORITY', inplace=True)

# Save to CSV
order_priority_count.to_csv('query_output.csv', index=False)
