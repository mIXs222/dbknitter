# filename: shipping_priority_query.py

from pymongo import MongoClient
from pandas import DataFrame
import direct_redis
import pandas as pd

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']
customers_coll = mongodb['customer']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379)
orders_df_str = redis_client.get('orders').decode('utf-8')
lineitem_df_str = redis_client.get('lineitem').decode('utf-8')

# Convert strings from Redis to DataFrames
orders_df = pd.read_json(orders_df_str)
lineitem_df = pd.read_json(lineitem_df_str)

# Query MongoDB for customers in the 'BUILDING' market segment
customers_df = pd.DataFrame(customers_coll.find({'C_MKTSEGMENT': 'BUILDING'}))

# Merge orders and lineitem dataframes based on 'O_ORDERKEY' and 'L_ORDERKEY'
merged_df = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate 'revenue' for each line in the lineitem dataframe
merged_df['revenue'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Select and filter data based on the given conditions
result_df = merged_df[merged_df['L_SHIPDATE'] > '1995-03-15']
result_df = result_df.groupby('O_ORDERKEY').agg({'revenue': 'sum', 'O_SHIPPRIORITY': 'max'}).reset_index()

# Merge result with customers dataframe based on 'C_CUSTKEY' and 'O_CUSTKEY' to get the final dataframe for customers in 'BUILDING' market segment
final_result_df = result_df.merge(customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Further sort the dataframe according to the requirements: in decreasing order of revenue
final_result_df.sort_values('revenue', ascending=False, inplace=True)

# Project the required columns into the final output and write to csv
final_output = final_result_df[['O_SHIPPRIORITY', 'revenue']]
final_output.to_csv('query_output.csv', index=False)
