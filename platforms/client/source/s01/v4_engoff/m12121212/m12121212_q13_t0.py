import pymongo
import pandas as pd
import direct_redis

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]
mongodb_orders_collection = mongodb_db["orders"]

# Fetch orders data from MongoDB
orders_query = {'$and': [
    {'O_ORDERSTATUS': {'$ne': 'pending'}},
    {'O_ORDERSTATUS': {'$ne': 'deposits'}},
    {'O_COMMENT': {'$not': {'$regex': 'pending|deposits', '$options': 'i'}}}
]}
orders_projection = {'_id': 0, 'O_CUSTKEY': 1}
mongodb_orders = pd.DataFrame(list(mongodb_orders_collection.find(orders_query, orders_projection)))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch customer data from Redis
customer_df = redis_client.get('customer')
if customer_df is not None:
    customer_df = pd.read_json(customer_df)
else:
    customer_df = pd.DataFrame(columns=[
        'C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'
    ])

# Prepare customer orders count DataFrame
customers_orders_count = mongodb_orders.groupby('O_CUSTKEY').size().reset_index(name='orders_count')
customers_orders_count['O_CUSTKEY'] = customers_orders_count['O_CUSTKEY'].astype(str)

# Merging customers with their order counts
results = customer_df.merge(customers_orders_count.rename(columns={'O_CUSTKEY': 'C_CUSTKEY'}), left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left').fillna(0)
results = results[['C_CUSTKEY', 'orders_count']].astype({'orders_count': 'int64'})
distribution = results['orders_count'].value_counts().sort_index().reset_index()
distribution.columns = ['number_of_orders', 'number_of_customers']

# Write results to CSV file
distribution.to_csv('query_output.csv', index=False)
