import pymongo
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_orders_collection = mongo_db["orders"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetching orders data from MongoDB
mongo_orders_df = pd.DataFrame(list(mongo_orders_collection.find({}, {
    "_id": 0, "O_ORDERKEY": 1, "O_CUSTKEY": 1, "O_ORDERSTATUS": 1, "O_COMMENT": 1
})))

# Filter out 'pending' or 'deposits' from order comments and exclude pending order statuses
filtered_orders = mongo_orders_df[~mongo_orders_df['O_COMMENT'].str.contains("pending|deposits", case=False, na=False)]
filtered_orders = filtered_orders[~filtered_orders['O_ORDERSTATUS'].str.contains("P")]

# Fetching customer data from Redis
customer_keys = redis_client.keys('customer:*')
customer_data = [eval(redis_client.get(key)) for key in customer_keys]
customer_df = pd.DataFrame(customer_data)

# Renaming Redis keys to match with MongoDB data keys
customer_df.rename(columns={
    "C_CUSTKEY": "O_CUSTKEY",
    "C_NAME": "name",
    "C_ADDRESS": "address",
    "C_NATIONKEY": "nationkey",
    "C_PHONE": "phone",
    "C_ACCTBAL": "acctbal",
    "C_MKTSEGMENT": "mktsegment",
    "C_COMMENT": "comment"
}, inplace=True)

# Counting orders by customer
orders_count_by_customer = filtered_orders.groupby('O_CUSTKEY').size().reset_index(name='order_count')

# Merging customers with orders count
customer_orders = pd.merge(customer_df, orders_count_by_customer, on='O_CUSTKEY', how='left')

# Filling 0 where there are no orders
customer_orders['order_count'].fillna(0, inplace=True)

# Counting the number of customers by order count
distribution = customer_orders.groupby('order_count').size().reset_index(name='customer_count')

# Writing results to CSV
distribution.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
