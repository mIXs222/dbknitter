# python_code.py
import pymongo
import pandas as pd
import direct_redis

# Connect to MongoDB
client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = client['tpch']
customer_collection = mongodb['customer']

# Get customer data
customer_data = pd.DataFrame(list(customer_collection.find({}, {"_id": 0})))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get orders data from Redis and convert it to a DataFrame
orders_data_string = redis_client.get('orders')
orders_data = pd.read_json(orders_data_string, orient='records')

# Filter out "pending" or "deposits" orders from the orders data
orders_data_filtered = orders_data[~orders_data['O_COMMENT'].str.contains('pending|deposits', na=False)]

# Count orders per customer
orders_per_customer = orders_data_filtered['O_CUSTKEY'].value_counts().reset_index()
orders_per_customer.columns = ['C_CUSTKEY', 'order_count']

# Merge customer data with order counts
merged_data = pd.merge(customer_data, orders_per_customer, how='left', left_on='C_CUSTKEY', right_on='C_CUSTKEY')
merged_data['order_count'].fillna(0, inplace=True)  # Replace NaN with 0 for customers with no orders

# Count the distribution of customers by number of orders
customer_order_distribution = merged_data['order_count'].value_counts().sort_index().reset_index()
customer_order_distribution.columns = ['num_orders', 'num_customers']

# Write the results to a CSV file
customer_order_distribution.to_csv('query_output.csv', index=False)
