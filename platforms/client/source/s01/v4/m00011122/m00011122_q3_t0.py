from pymongo import MongoClient
import pandas as pd
import redis
from direct_redis import DirectRedis
from datetime import datetime
from functools import reduce

# MongoDB connection
client = MongoClient("mongodb", 27017)
mongodb = client.tpch
customer_df = pd.DataFrame(list(mongodb.customer.find(
    {'C_MKTSEGMENT': 'BUILDING'},
    {'_id': 0, 'C_CUSTKEY': 1}
)))

# Redis connection
r = DirectRedis(host='redis', port=6379, db=0)
orders_df = pd.read_json(r.get('orders'))
lineitem_df = pd.read_json(r.get('lineitem'))

# Filter redis dataframes based on dates
orders_df = orders_df[orders_df['O_ORDERDATE'] < '1995-03-15']
lineitem_df = lineitem_df[lineitem_df['L_SHIPDATE'] > '1995-03-15']

# Merging dataframes
merge_conditions = [
    customer_df.set_index('C_CUSTKEY'),
    orders_df.set_index('O_CUSTKEY'),
    lineitem_df.set_index('L_ORDERKEY')
]
result_df = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True, how='inner'), merge_conditions)

# Calculating Revenue
result_df['REVENUE'] = result_df.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1)

# Group by and aggregate
output_df = result_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])\
                     .agg({'REVENUE': 'sum'})\
                     .reset_index()

# Sort the results
output_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Write to CSV
output_df.to_csv('query_output.csv', index=False)
