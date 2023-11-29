import pymongo
import redis
import pandas as pd

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
customer_collection = mongo_db["customer"]

# Get all customers from MongoDB
customer_data = pd.DataFrame(list(customer_collection.find()))

# Redis Connection
redis_client = redis.StrictRedis(host='redis', port=6379, db=0)

# Load the orders data - Assuming the `DirectRedis` class works similarly to `StrictRedis`
order_str = redis_client.get('orders')
orders_data = pd.read_parquet(order_str, engine='pyarrow')

# Filter orders not containing 'pending' or 'deposits'
orders_filtered = orders_data[~orders_data['O_COMMENT'].str.contains('pending|deposits', regex=True)]

# Count the orders per customer
order_count_per_customer = orders_filtered['O_CUSTKEY'].value_counts().reset_index()
order_count_per_customer.columns = ['C_CUSTKEY', 'order_count']

# Join with customer data to get number of orders per customer
customer_order_counts = pd.merge(customer_data, order_count_per_customer, how='left', left_on='C_CUSTKEY', right_on='C_CUSTKEY')
customer_order_counts['order_count'] = customer_order_counts['order_count'].fillna(0).astype('int64')

# Calculate the distribution of customers by the number of orders
customer_distribution = customer_order_counts['order_count'].value_counts().reset_index()
customer_distribution.columns = ['number_of_orders', 'number_of_customers']

# Write the output to CSV
customer_distribution.to_csv('query_output.csv', index=False)
