from pymongo import MongoClient
import direct_redis
import pandas as pd
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']
orders_col = mongodb['orders']

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query MongoDB for orders with O_ORDERPRIORITY '1-URGENT' or '2-HIGH'
critical_priority_orders = list(orders_col.find({
    'O_ORDERPRIORITY': {'$in': ['1-URGENT', '2-HIGH']},
    'O_ORDERDATE': {'$gte': datetime(1994, 1, 1), '$lt': datetime(1995, 1, 1)}
}, {'_id': 0, 'O_ORDERKEY': 1}))

critical_order_keys = [order['O_ORDERKEY'] for order in critical_priority_orders]

# Query Redis for lineitems
lineitems_str = r.get('lineitem')
lineitems_df = pd.read_json(lineitems_str, orient='index')

# Process lineitems to meet the conditions
lineitems_df['L_SHIPDATE'] = pd.to_datetime(lineitems_df['L_SHIPDATE'])
lineitems_df['L_RECEIPTDATE'] = pd.to_datetime(lineitems_df['L_RECEIPTDATE'])
lineitems_df['L_COMMITDATE'] = pd.to_datetime(lineitems_df['L_COMMITDATE'])
lineitems_df = lineitems_df[lineitems_df['L_ORDERKEY'].isin(critical_order_keys)]
lineitems_df = lineitems_df[(lineitems_df['L_RECEIPTDATE'] >= datetime(1994, 1, 1)) & 
                            (lineitems_df['L_RECEIPTDATE'] < datetime(1995, 1, 1)) & 
                            (lineitems_df['L_SHIPDATE'] < lineitems_df['L_COMMITDATE'])]

# Group by ship mode and priority
grouped = lineitems_df.groupby(['L_SHIPMODE', 'O_ORDERPRIORITY'])

# Perform the count
result_df = grouped.size().reset_index(name='COUNT')

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
