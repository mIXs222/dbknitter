# query.py

from pymongo import MongoClient
from direct_redis import DirectRedis
import pandas as pd
from datetime import datetime, timedelta

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from Redis
customers_data = redis_client.get('customer')
customers_df = pd.read_json(customers_data)

# Process customer data to filter country codes and account balances more than 0
filtered_customers = customers_df[
    customers_df['C_PHONE'].str[:2].isin(['20', '40', '22', '30', '39', '42', '21'])
    & (customers_df['C_ACCTBAL'] > 0)
]

# Calculate the average account balance for the filtered customers
average_balance = filtered_customers['C_ACCTBAL'].mean()

# Get customers with account balance greater than average
target_customers = filtered_customers[filtered_customers['C_ACCTBAL'] > average_balance]

# Get orders from the last 7 years
seven_years_ago = datetime.now() - timedelta(days=7*365)
orders_df = pd.DataFrame(list(orders_collection.find(
    {"O_ORDERDATE": {"$gte": seven_years_ago.strftime('%Y-%m-%d')}}
)))

# Find customers who have not placed orders in the last 7 years
customers_not_ordered = target_customers[~target_customers['C_CUSTKEY'].isin(orders_df['O_CUSTKEY'])]

# Group by country codes
result = customers_not_ordered.groupby(customers_not_ordered['C_PHONE'].str[:2]) \
    .agg(number_of_customers=('C_CUSTKEY', 'count'), total_account_balance=('C_ACCTBAL', 'sum')) \
    .reset_index().rename(columns={'C_PHONE': 'CNTRYCODE'})

# Sorting the result
result.sort_values(by='CNTRYCODE', inplace=True)

# Output to file
result.to_csv('query_output.csv', index=False)
