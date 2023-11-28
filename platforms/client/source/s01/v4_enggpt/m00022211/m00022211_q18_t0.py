# query.py
import pymongo
import pandas as pd
import direct_redis

# Function to connect to MongoDB
def connect_mongodb(host, port, db_name):
    client = pymongo.MongoClient(host, port)
    db = client[db_name]
    return db

# Function to connect to Redis, assuming direct_redis is a custom module for this context
def connect_redis(host, port, db_name):
    return direct_redis.DirectRedis(host=host, port=port, db=int(db_name))

# Connecting to MongoDB
db_mongo = connect_mongodb("mongodb", 27017, "tpch")

# Fetching data from MongoDB collections
orders_df = pd.DataFrame(list(db_mongo.orders.find()))
lineitem_df = pd.DataFrame(list(db_mongo.lineitem.find()))

# Connecting to Redis
redis_client = connect_redis("redis", 6379, "0")

# Fetching data from Redis. Assuming get returns a dataframe.
customer_df = redis_client.get('customer')

# Converting the Redis customer dataframe id columns to match the naming convention used in the SQL queries
customer_df.rename(columns={'C_CUSTKEY': 'O_CUSTKEY'}, inplace=True)

# Joining the customer DataFrame with the orders DataFrame on customer key
customer_orders = pd.merge(customer_df, orders_df, on="O_CUSTKEY")

# Grouping by L_ORDERKEY and summing L_QUANTITY to find total quantity per order
lineitem_grouped = lineitem_df.groupby('L_ORDERKEY')['L_QUANTITY'].sum().reset_index()
# Filter the order keys with total quantity > 300
lineitem_filtered = lineitem_grouped[lineitem_grouped['L_QUANTITY'] > 300]

# Join the orders with the line items that have total quantity > 300
matching_orders = orders_df[orders_df['O_ORDERKEY'].isin(lineitem_filtered['L_ORDERKEY'])]

# Join the matching orders with customer_orders
final_result = pd.merge(matching_orders, customer_orders, on="O_ORDERKEY")

# Selecting necessary columns for the final output
final_result = final_result[['C_NAME', 'O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Group by the necessary columns and sort by total price and order date as per the query requirements
final_result = final_result.groupby(['C_NAME', 'O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({'L_QUANTITY':'sum'}).reset_index()
final_result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Writing results to query_output.csv
final_result.to_csv('query_output.csv', index=False)
