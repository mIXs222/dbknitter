import pandas as pd
from pymongo import MongoClient
from direct_redis import DirectRedis
import csv

# MongoDB connection and data retrieval
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customers = pd.DataFrame(list(mongo_db.customer.find()))

# Redis connection and data retrieval
redis_client = DirectRedis(host='redis', port=6379, db=0)
orders_df = pd.read_json(redis_client.get('orders'), orient='index')

# Convert 'orders' DataFrame to proper format
orders = orders_df.T.reset_index(drop=True)

# Extract country code from phone numbers
customers['CNTRYCODE'] = customers['C_PHONE'].str[:2]

# Calculate the average account balance for each country with positive balances
avg_account_balances = customers[customers['C_ACCTBAL'] > 0].groupby('CNTRYCODE')['C_ACCTBAL'].mean().to_dict()

# Filter customers with account balance > average in specific countries
target_countries = ['20', '40', '22', '30', '39', '42', '21']
customers = customers[
    customers.apply(lambda x: x['C_ACCTBAL'] > avg_account_balances.get(x['CNTRYCODE'], 0)
                                 and x['CNTRYCODE'] in target_countries, axis=1)]

# Exclude customers who have placed orders
customer_keys_with_orders = set(orders['O_CUSTKEY'])
customers = customers[~customers['C_CUSTKEY'].isin(customer_keys_with_orders)]

# Group by country code and aggregate the results
result = customers.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index().sort_values('CNTRYCODE')

# Output the result to a file
result.to_csv('query_output.csv', index=False)
