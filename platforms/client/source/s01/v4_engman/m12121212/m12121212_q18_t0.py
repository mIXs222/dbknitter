import pymongo
import pandas as pd
import redis
import direct_redis
import csv

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_orders = mongo_db["orders"]

# Fetch orders from MongoDB and filter by total quantity
mongo_orders_df = pd.DataFrame(list(mongo_orders.find({}, {
    "_id": 0,
    "O_ORDERKEY": 1,
    "O_CUSTKEY": 1,
    "O_ORDERDATE": 1,
    "O_TOTALPRICE": 1
})))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch customer and lineitem tables from Redis
customer_df = pd.read_json(redis_client.get('customer'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter lineitems with total quantity greater than 300 and group by order key
lineitem_grouped_df = lineitem_df[lineitem_df['L_QUANTITY'] > 300].groupby('L_ORDERKEY').sum().reset_index()

# Merge the order info with the large quantity lineitems
large_orders_df = pd.merge(mongo_orders_df, lineitem_grouped_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Merge the large orders with customer info and select the necessary columns
result_df = pd.merge(large_orders_df, customer_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
result_df = result_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Sort the results by O_TOTALPRICE descending and O_ORDERDATE ascending
result_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Write the results to a CSV file: query_output.csv
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
