import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_orders_collection = mongo_db["orders"]

# Redis Connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MongoDB
mongo_orders = list(mongo_orders_collection.find({}, 
                      {'_id': 0, 'O_ORDERKEY': 1, 'O_CUSTKEY': 1, 'O_ORDERSTATUS': 1, 'O_COMMENT': 1}))
orders_df = pd.DataFrame(mongo_orders)

# Perform query for MongoDB data
orders_df = orders_df[~orders_df['O_ORDERSTATUS'].isin(['pending', 'deposits'])]
orders_df = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', na=False)]

# Fetch data from Redis
customer_data = redis_client.get('customer')
customer_df = pd.read_json(customer_data)

# Perform a left join for combining the customer data with the orders data
combined_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Group the combined data by customer and count the number of orders
customer_order_count = combined_df.groupby('C_CUSTKEY')['O_ORDERKEY'].count().reset_index(name='order_count')

# Get the distribution of customers by the number of orders they have made
customer_distribution = customer_order_count.groupby('order_count').size().reset_index(name='num_customers')

# Write results to CSV
customer_distribution.to_csv('query_output.csv', index=False)
