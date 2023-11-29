from pymongo import MongoClient
import direct_redis
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Connect to the MongoDB instance
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# Get current date and go back 7 years
seven_years_ago = datetime.now() - timedelta(days=7*365)

# Convert the collection to Pandas DataFrame
orders_df = pd.DataFrame(list(orders_collection.find(
    {
        "O_ORDERDATE": {"$lt": seven_years_ago},
        "O_TOTALPRICE": {"$gt": 0.0}
    },
    {
        "_id": False,
        "O_CUSTKEY": True
    }
)))

# Orders made by customers in the last 7 years will be excluded
excluded_customers = set(orders_df['O_CUSTKEY'])

# Connect to the Redis instance
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get 'customer' table from Redis as a DataFrame
customer_json = redis_client.get('customer')
customer_df = pd.read_json(customer_json, orient='records')

# Process the dataframe
filtered_customer_df = customer_df[
    (~customer_df['C_CUSTKEY'].isin(excluded_customers)) & 
    (customer_df['C_PHONE'].str.slice(0, 2).isin(['20', '40', '22', '30', '39', '42', '21'])) &
    (customer_df['C_ACCTBAL'] > 0.0)
]

# Compute the average account balance
average_acct_balance = filtered_customer_df['C_ACCTBAL'].mean()

# Filter customers with account balance greater than the average
target_customers_df = filtered_customer_df[filtered_customer_df['C_ACCTBAL'] > average_acct_balance]

# Select country code and calculate aggregated values
result_df = target_customers_df.groupby(target_customers_df['C_PHONE'].str.slice(0, 2)).agg(
    num_customers=pd.NamedAgg(column="C_CUSTKEY", aggfunc="count"),
    total_acct_balance=pd.NamedAgg(column="C_ACCTBAL", aggfunc="sum")
).reset_index()

result_df.columns = ['CNTRYCODE', 'NUM_CUSTOMERS', 'TOTAL_ACCT_BALANCE']

# Sort the results
result_df.sort_values(by='CNTRYCODE', inplace=True)

# Write the results to query_output.csv
result_df.to_csv('query_output.csv', index=False)
