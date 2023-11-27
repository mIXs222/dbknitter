import pymongo
import redis
import pandas as pd
from direct_redis import DirectRedis
from bson.objectid import ObjectId

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_customers = mongo_db["customer"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379)

# Retrieve data from MongoDB
customers_df = pd.DataFrame(list(mongo_customers.find({}, {'_id': 0})))

# Retrieve data from Redis (assuming data is stored in a format compatible with pandas)
orders_df = pd.DataFrame(redis_client.get('orders'))

# Merge data frames based on customer key
merged_df = pd.merge(customers_df, orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Create a DataFrame to count the number of orders per customer, excluding 'pending' and 'deposits' in the comments
order_counts = (
    merged_df[~(merged_df['O_COMMENT'].str.contains('pending', na=False) | 
                merged_df['O_COMMENT'].str.contains('deposits', na=False))]
    .groupby('C_CUSTKEY')
    .size()
    .reset_index(name='number_of_orders')
)

# Include customers with no orders
all_customers_orders_count = (
    customers_df[['C_CUSTKEY']]
    .merge(order_counts, how='left', left_on='C_CUSTKEY', right_on='C_CUSTKEY')
    .fillna(0)
)

# Count the number of customers per number of orders
distribution = (
    all_customers_orders_count['number_of_orders']
    .value_counts()
    .reset_index()
    .rename(columns={'index': 'orders_count', 'number_of_orders': 'customer_count'})
    .sort_values(by='orders_count')
)

# Write the result to a CSV file
distribution.to_csv('query_output.csv', index=False)
