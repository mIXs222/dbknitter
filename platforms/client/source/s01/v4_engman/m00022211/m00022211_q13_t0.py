# Save this code in a file named `query_exec.py`

import pymongo
import pandas as pd
import direct_redis

# MongoDB connection and data retrieval
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
orders_collection = mongo_db["orders"]

# Query for MongoDB to exclude orders with "pending" or "deposits" in the comment
orders_data = list(orders_collection.find(
    {
        "O_ORDERSTATUS": {"$nin": ["P", "D"]},
        "O_COMMENT": {"$not": {"$regex": ".*pending.*deposits.*"}}
    },
    {
        "_id": 0, "O_ORDERKEY": 1, "O_CUSTKEY": 1
    }
))

# Convert MongoDB orders data to DataFrame
orders_df = pd.DataFrame(orders_data)

# Redis connection and data retrieval
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read the 'customer' DataFrame stored in Redis
customer_df = pd.read_json(redis_connection.get('customer'), orient='records')

# Compute the number of orders per customer
orders_per_customer = orders_df.groupby('O_CUSTKEY').size().reset_index(name='number_of_orders')

# Compute the distribution of customers by the number of orders they have made
customer_order_distribution = orders_per_customer.groupby('number_of_orders').size().reset_index(name='number_of_customers')

# Write the final dataframe to CSV
customer_order_distribution.to_csv('query_output.csv', index=False)
