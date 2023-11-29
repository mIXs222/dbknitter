import pymongo
import redis
import pandas as pd
from itertools import groupby

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
db = client['tpch']
mongodb_orders_col = db['orders']

# Connect to Redis
r = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Get orders data from MongoDB
orders_data = list(mongodb_orders_col.find({}, {'_id': 0, 'O_CUSTKEY': 1, 'O_COMMENT': 1}))

# Get customer data keys from Redis
customer_keys = r.keys('customer:*')

# Get customer data from Redis
customers_data = []
for key in customer_keys:
    customers_data.append(r.hgetall(key))

# Convert Redis data to DataFrame
customers_df = pd.DataFrame(customers_data)

# Convert MongoDB data to DataFrame
orders_df = pd.DataFrame(orders_data)

# Filter out the orders with 'pending' and 'deposits' in the comment
filtered_orders_df = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', regex=True)]

# Group by 'O_CUSTKEY' and count the number of orders per customer
orders_count_per_customer = filtered_orders_df.groupby('O_CUSTKEY').size().reset_index(name='order_count')

# Count the number of customers per order count
distribution = orders_count_per_customer.groupby('order_count').size().reset_index(name='customer_count')

# Renaming columns to match the query output
distribution = distribution.rename(columns={'order_count': 'number_of_orders', 'customer_count': 'number_of_customers'})

# Write result to CSV
distribution.to_csv('query_output.csv', index=False)
