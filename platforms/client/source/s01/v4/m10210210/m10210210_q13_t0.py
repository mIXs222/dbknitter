import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to the MongoDB server
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']

# Get all orders that do not include 'pending%deposits' in the comments
orders_df = pd.DataFrame(list(orders_collection.find({"O_COMMENT": {"$not": {"$regex": "pending%deposits"}}},
                                                      {"_id": 0, "O_ORDERKEY": 1, "O_CUSTKEY": 1})))

# Connect to the Redis server
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get customer data from Redis
customer_df = pd.read_json(redis_client.get('customer'), orient='records')

# Perform the LEFT OUTER JOIN operation similar to SQL
merged_df = pd.merge(customer_df, orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Group by C_CUSTKEY and count the orders
c_orders = merged_df.groupby('C_CUSTKEY')['O_ORDERKEY'].agg('count').reset_index(name='C_COUNT')

# Now, group by C_COUNT to get the distribution
custdist_df = c_orders.groupby('C_COUNT')['C_COUNT'].agg(CUSTDIST='count').reset_index()

# Order the result as specified
custdist_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False], inplace=True)

# Write the result to a CSV file
custdist_df.to_csv('query_output.csv', index=False)
