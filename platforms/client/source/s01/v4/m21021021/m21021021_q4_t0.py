import pymongo
import pandas as pd
import csv
from direct_redis import DirectRedis

# MongoDB connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client['tpch']
lineitem = mongodb['lineitem']

# Redis connection
redis = DirectRedis(host='redis', port=6379, db=0)

# Fetch orders from Redis
orders_str = redis.get('orders')
orders_df = pd.read_json(orders_str)

# Filter orders DataFrame for the required date range
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
filtered_orders = orders_df[
    (orders_df['O_ORDERDATE'] >= '1993-07-01') &
    (orders_df['O_ORDERDATE'] < '1993-10-01')
]

# Fetch lineitem data from MongoDB and create a DataFrame
lineitem_cursor = lineitem.find()
lineitem_df = pd.DataFrame(list(lineitem_cursor))

# Convert to datetime for comparison
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Check for existence in lineitem and filter orders with condition
eligible_orders = filtered_orders[
    filtered_orders['O_ORDERKEY'].isin(
        lineitem_df.loc[
            lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE'],
            'L_ORDERKEY'
        ]
    )
]

# Group and count based on 'O_ORDERPRIORITY'
result = eligible_orders.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

# Sort the final result by 'O_ORDERPRIORITY'
result.sort_values('O_ORDERPRIORITY', ascending=True, inplace=True)

# Write the result to CSV
result.to_csv('query_output.csv', index=False)
