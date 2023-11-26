from pymongo import MongoClient
import pandas as pd
import redis
import direct_redis
import csv

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MongoDB 'orders' collection
orders_df = pd.DataFrame(list(mongodb.orders.find({})))

# Fetch data from Redis 'customer' collection
customer_df = pd.DataFrame(redis_client.get('customer'))

# Perform the equivalent query on the dataframes
# Calculate average account balance for specified countries with positive account balance
countries = ('20', '40', '22', '30', '39', '42', '21')
avg_acctbal = customer_df[
    (customer_df.C_ACCTBAL > 0.00) &
    (customer_df.C_PHONE.str[:2].isin(countries))
].C_ACCTBAL.mean()

# Select customers with an account balance above the average and in the specified countries
selected_cust_df = customer_df[
    (customer_df.C_ACCTBAL > avg_acctbal) &
    (customer_df.C_PHONE.str[:2].isin(countries))
]

# Select orders that do not exist for the customers
orders_cust_keys = orders_df.O_CUSTKEY.unique()
selected_cust_df = selected_cust_df[~selected_cust_df.C_CUSTKEY.isin(orders_cust_keys)]

# Apply the group by and aggregation
query_output = selected_cust_df.groupby(selected_cust_df.C_PHONE.str[:2]).agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index().rename(columns={'C_PHONE': 'CNTRYCODE'})

# Save the output to a CSV file
query_output.to_csv('query_output.csv', index=False)

# Close connections
mongo_client.close()
redis_client.close()
