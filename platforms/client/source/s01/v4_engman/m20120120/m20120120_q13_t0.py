# Python code to execute the query (query_code.py)
from pymongo import MongoClient
import pandas as pd
import direct_redis
import re

# Connect to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
mongo_customers = mongo_db['customer']
mongo_customers_df = pd.DataFrame(list(mongo_customers.find({}, {'_id': 0})))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
redis_orders = redis_client.get('orders')
redis_orders_df = pd.read_json(redis_orders)

# Filter out orders with 'pending' and 'deposits' in O_COMMENT
filtered_orders = redis_orders_df[~redis_orders_df['O_COMMENT'].str.contains('pending|deposits', flags=re.IGNORECASE, regex=True)]

# Join the dataframes on the customer key
merged_df = pd.merge(mongo_customers_df, filtered_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='inner')

# Group by customer key and count the number of orders each customer has made
grouped = merged_df.groupby('C_CUSTKEY').size().reset_index(name='orders_count')

# Get the distribution of customers by the number of orders they have made
customer_order_distribution = grouped.groupby('orders_count').size().reset_index(name='customer_count')

# Write the result to the 'query_output.csv' file
customer_order_distribution.to_csv('query_output.csv', index=False)
