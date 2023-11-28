import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]

# Read 'customer' and 'lineitem' collections
customer_df = pd.DataFrame(list(mongodb.customer.find()))
lineitem_df = pd.DataFrame(list(mongodb.lineitem.find()))

# Connect to redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Read 'nation' and 'orders' tables from redis
nation_df = pd.read_msgpack(redis_client.get('nation'))
orders_df = pd.read_msgpack(redis_client.get('orders'))

# Filter orders by date range and join with customers
start_date = datetime(1993, 10, 1)
end_date = datetime(1993, 12, 31)
filtered_orders_df = orders_df[
    (pd.to_datetime(orders_df['O_ORDERDATE']) >= start_date) &
    (pd.to_datetime(orders_df['O_ORDERDATE']) <= end_date)
]

# Join orders with customers
orders_customers_df = pd.merge(
    filtered_orders_df,
    customer_df,
    how='inner',
    left_on='O_CUSTKEY',
    right_on='C_CUSTKEY'
)

# Filter lineitem by return flag 'R' and join with the orders_customers
lineitem_filtered_df = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R']
combined_df = pd.merge(
    orders_customers_df,
    lineitem_filtered_df,
    how='inner',
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY'
)

# Join with nation
final_df = pd.merge(
    combined_df,
    nation_df,
    how='inner',
    left_on='C_NATIONKEY',
    right_on='N_NATIONKEY'
)

# Select needed columns and calculate revenue
final_df['REVENUE'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])
result_df = final_df[[
    'C_CUSTKEY',
    'C_NAME',
    'REVENUE',
    'C_ACCTBAL',
    'N_NAME',
    'C_ADDRESS',
    'C_PHONE',
    'C_COMMENT'
]].copy()

# Group by necessary columns
grouped_result_df = result_df.groupby(
    ['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']
).agg({'REVENUE': 'sum'}).reset_index()

# Sort the results
sorted_df = grouped_result_df.sort_values(
    by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
    ascending=[True, True, True, False]
)

# Write results to CSV
sorted_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
