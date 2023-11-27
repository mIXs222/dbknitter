import pymongo
import redis
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# MongoDB settings
mongo_host = 'mongodb'
mongo_port = 27017
mongo_dbname = 'tpch'

# Redis settings
redis_host = 'redis'
redis_port = 6379
redis_dbname = '0'

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host=mongo_host, port=mongo_port)
mongo_db = mongo_client[mongo_dbname]

# Connect to Redis
redis_client = DirectRedis(host=redis_host, port=redis_port)

# Loading customer data from MongoDB
customer_collection = mongo_db['customer']
customers_df = pd.DataFrame(list(customer_collection.find({'C_MKTSEGMENT': 'BUILDING'}, {'_id': 0})))

# Loading lineitem data from MongoDB
lineitem_collection = mongo_db['lineitem']
lineitems_df = pd.DataFrame(list(lineitem_collection.find({
    'L_SHIPDATE': {'$gt': datetime(1995, 3, 15)}
}, {'_id': 0})))

# Loading orders from Redis and converting to a pandas DataFrame
orders = redis_client.get('orders')
orders_df = pd.read_json(orders, orient='index')

# Filter orders by date
orders_df = orders_df[orders_df['O_ORDERDATE'] < datetime(1995, 3, 15)]

# Joining the dataframes
df_merge = customers_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df_merge = df_merge.merge(lineitems_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculating revenue
df_merge['REVENUE'] = df_merge['L_EXTENDEDPRICE'] * (1 - df_merge['L_DISCOUNT'])

# Grouping and sorting
result_df = df_merge.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg({
    'REVENUE': 'sum'
}).reset_index().sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Writing result to CSV file
result_df.to_csv('query_output.csv', index=False)
