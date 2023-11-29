import pymongo
import pandas as pd
import redis
import direct_redis
from datetime import datetime, timedelta

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_customers = mongo_db["customer"]

# Redis Connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get Redis orders data
orders_data = redis_client.get_df('orders')
orders_data['O_ORDERDATE'] = pd.to_datetime(orders_data['O_ORDERDATE'])

# Get MongoDB customers data
customers_data = pd.DataFrame(list(mongo_customers.find({})))

# Filter customers based on the phone's country code
selected_countries = ['20', '40', '22', '30', '39', '42', '21']
customers_data['CNTRYCODE'] = customers_data['C_PHONE'].str[:2]
customers_filtered = customers_data[customers_data['CNTRYCODE'].isin(selected_countries)]

# Determine the cutoff date 7 years ago
cutoff_date = datetime.now() - timedelta(days=7*365)

# Check which customers have not placed orders for 7 years
active_customers = orders_data[orders_data['O_ORDERDATE'] >= cutoff_date]['O_CUSTKEY'].unique()
inactive_customers = customers_filtered[~customers_filtered['C_CUSTKEY'].isin(active_customers)]

# Determine the average account balance of customers from the filtered countries
avg_acct_balance = customers_filtered[customers_filtered['C_ACCTBAL'] > 0.00]['C_ACCTBAL'].mean()

# Filter customers with account balance greater than average
target_customers = inactive_customers[inactive_customers['C_ACCTBAL'] > avg_acct_balance]

# Group by country code, count the customers, and sum the account balances
result = target_customers.groupby('CNTRYCODE').agg(
    num_customers=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    total_acct_balance=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index()

# Sort by country code ascending
result_sorted = result.sort_values('CNTRYCODE', ascending=True)

# Output to csv
result_sorted.to_csv('query_output.csv', index=False)
