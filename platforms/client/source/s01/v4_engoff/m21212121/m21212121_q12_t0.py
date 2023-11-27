from pymongo import MongoClient
import direct_redis
import pandas as pd
from datetime import datetime

# MongoDB connection
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']
lineitem = mongodb['lineitem']

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
orders_data = redis_client.get('orders')

# Convert orders data from Redis into Pandas DataFrame
orders_df = pd.read_json(orders_data)

# Filtering orders with priority URGENT or HIGH
high_priority_orders = orders_df[(orders_df['O_ORDERPRIORITY'] == 'URGENT') |
                                 (orders_df['O_ORDERPRIORITY'] == 'HIGH')]

# Get the data from mongoDB
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)
query = {
    'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
    'L_RECEIPTDATE': {'$gte': start_date, '$lt': end_date},
    'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
    'L_COMMITDATE': {'$lt': 'L_RECEIPTDATE'},
}
projection = {
    '_id': False,
    'L_ORDERKEY': True,
    'L_SHIPMODE': True,
    'L_COMMITDATE': True,
    'L_RECEIPTDATE': True,
}

late_lineitems_cursor = lineitem.find(query, projection)
late_lineitems_df = pd.DataFrame(list(late_lineitems_cursor))

# Merging filtered orders with lineitems on O_ORDERKEY=L_ORDERKEY
result_df = pd.merge(high_priority_orders, late_lineitems_df,
                     left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Count per ship mode and priority
ship_mode_priority_count = result_df.groupby(['L_SHIPMODE', 'O_ORDERPRIORITY']).size().reset_index(name='COUNT')

# Output the results to a csv file
ship_mode_priority_count.to_csv('query_output.csv', index=False)
