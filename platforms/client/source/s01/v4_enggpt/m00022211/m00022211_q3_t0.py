# file: query_processing.py
import pymongo
import pandas as pd
from bson import ObjectId
import datetime
import direct_redis

# Connect to MongoDB (tpch database)
client = pymongo.MongoClient("mongodb", 27017)
db = client.tpch

# Retrieve 'orders' and 'lineitem' from MongoDB
orders_columns = ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']
lineitem_columns = ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']

orders_query = db.orders.find({'O_ORDERDATE': {'$lt': datetime.datetime(1995, 3, 15)}})
orders_df = pd.DataFrame(list(orders_query), columns=orders_columns)

lineitem_query = db.lineitem.find({'L_SHIPDATE': {'$gt': datetime.datetime(1995, 3, 15)}})
lineitem_df = pd.DataFrame(list(lineitem_query), columns=lineitem_columns)

# Connect to Redis
r = direct_redis.DirectRedis(host="redis", port=6379, db=0)

# Retrieve 'customer' from Redis
customer_data = r.get('customer')
customer_df = pd.read_json(customer_data, orient='records')

# Apply filters and calculations
filtered_customers = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']
filtered_orders = orders_df.merge(filtered_customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Calculate revenue and filter data based on the joined condition
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
valid_lineitems = lineitem_df[lineitem_df['L_ORDERKEY'].isin(filtered_orders['O_ORDERKEY'])]

# Join tables and calculate revenue
result = valid_lineitems.merge(filtered_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
result = result[['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'REVENUE']]
result_grouped = result.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'], as_index=False)['REVENUE'].sum()

# Sort the result
result_sorted = result_grouped.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Output the query result to CSV file 
result_sorted.to_csv('query_output.csv', index=False)
