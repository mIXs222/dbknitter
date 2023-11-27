# Python code (query_execution.py)

import pandas as pd
from pymongo import MongoClient
import redis
from direct_redis import DirectRedis

# MongoDB connection details
mongo_conn_params = {
    'host': 'mongodb',
    'port': 27017,
    'db_name': 'tpch'
}

# Redis connection details
redis_conn_params = {
    'db_name': 0,
    'port': 6379,
    'host': 'redis'
}

# Connect to MongoDB
mongo_client = MongoClient(host=mongo_conn_params['host'], port=mongo_conn_params['port'])
mongo_db = mongo_client[mongo_conn_params['db_name']]
mongo_orders_collection = mongo_db['orders']

# Fetch orders from MongoDB
orders_df = pd.DataFrame(list(mongo_orders_collection.find({}, {'O_CUSTKEY': 1, '_id': 0})))

# Connect to Redis
redis_client = DirectRedis(host=redis_conn_params['host'], port=redis_conn_params['port'], db=redis_conn_params['db_name'])

# Fetch customer data from Redis and store in DataFrame
customers_data = redis_client.get('customer')
customers_df = pd.read_msgpack(customers_data)

# Filter customers according to given conditions
countries_of_interest = ('20', '40', '22', '30', '39', '42', '21')
customers_df['CNTRYCODE'] = customers_df['C_PHONE'].str[:2]
selected_customers_df = customers_df[
    customers_df['CNTRYCODE'].isin(countries_of_interest) & 
    (customers_df['C_ACCTBAL'] > 0.00) &
    ~customers_df['C_CUSTKEY'].isin(orders_df['O_CUSTKEY'])
]

# Calculate average account balance
avg_acct_balance = selected_customers_df[selected_customers_df['C_ACCTBAL'] > 0.00]['C_ACCTBAL'].mean()

# Filter customers above average balance
above_avg_customers_df = selected_customers_df[selected_customers_df['C_ACCTBAL'] > avg_acct_balance]

# Perform aggregation based on country code
result_df = above_avg_customers_df.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index()

# Sort by country code
result_df.sort_values(by='CNTRYCODE', inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
