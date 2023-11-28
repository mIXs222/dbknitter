from pymongo import MongoClient
import direct_redis
import pandas as pd

# Constants
MONGO_HOSTNAME = 'mongodb'
MONGO_PORT = 27017
MONGO_DB_NAME = 'tpch'
REDIS_HOSTNAME = 'redis'
REDIS_PORT = 6379
REDIS_DB_NAME = 0

# Connecting to MongoDB
mongo_client = MongoClient(host=MONGO_HOSTNAME, port=MONGO_PORT)
mongo_db = mongo_client[MONGO_DB_NAME]
customer_collection = mongo_db['customer']

# Connecting to Redis
redis_client = direct_redis.DirectRedis(host=REDIS_HOSTNAME, port=REDIS_PORT, db=REDIS_DB_NAME)

# Fetch customer data from MongoDB
customer_data = list(customer_collection.find({}, {'_id': 0}))
customer_df = pd.DataFrame(customer_data)
customer_df['CNTRYCODE'] = customer_df['C_PHONE'].str[:2]

# Calculate average account balance for customers with positive balances and specified country codes
specified_country_codes = ['20', '40', '22', '30', '39', '42', '21']
positive_balances_df = customer_df[
    (customer_df['C_ACCTBAL'] > 0) & customer_df['CNTRYCODE'].isin(specified_country_codes)]
avg_balances = positive_balances_df.groupby('CNTRYCODE')['C_ACCTBAL'].mean().to_dict()

# Apply filters for account balance and country codes
filtered_customers = customer_df[
    (customer_df['C_ACCTBAL'] > customer_df['CNTRYCODE'].map(avg_balances)) & 
    customer_df['CNTRYCODE'].isin(specified_country_codes)]

# Fetch order data from Redis
orders_df = pd.read_json(redis_client.get('orders'), orient='records')

# Exclude customers who have placed orders
customers_without_orders = filtered_customers[
    ~filtered_customers['C_CUSTKEY'].isin(orders_df['O_CUSTKEY'])]

# Perform aggregation
custsale_df = customers_without_orders.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index().sort_values('CNTRYCODE')

# Write results to CSV
custsale_df.to_csv('query_output.csv', index=False)
