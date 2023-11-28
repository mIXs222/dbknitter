import pandas as pd
import pymongo
from bson.regex import Regex
from direct_redis import DirectRedis

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['tpch']
orders = pd.DataFrame(list(mongo_db['orders'].find()))

# Redis Connection
redis = DirectRedis(host='redis', port=6379, db=0)
customer_data = eval(redis.get('customer'))
customer = pd.DataFrame.from_dict(customer_data)

# Extract country codes from phone numbers and filter customers by those codes
customer['CNTRYCODE'] = customer['C_PHONE'].apply(lambda x: x[:2])

# Calculate the average account balance for customers with positive balances in the specified country codes
specific_codes = ['20', '40', '22', '30', '39', '42', '21']
avg_acct_balance = customer[customer['C_ACCTBAL'] > 0].groupby('CNTRYCODE')['C_ACCTBAL'].mean().reset_index()
avg_acct_balance = avg_acct_balance[avg_acct_balance['CNTRYCODE'].isin(specific_codes)]

# Filter only customers with positive balances greater than the country average
customers_above_avg = customer.merge(avg_acct_balance, on='CNTRYCODE', suffixes=('', '_AVG'))
customers_above_avg = customers_above_avg[customers_above_avg['C_ACCTBAL'] > customers_above_avg['C_ACCTBAL_AVG']]

# Exclude customers who have placed orders
orders_customers = orders[['O_CUSTKEY']].drop_duplicates()
customers_no_orders = customers_above_avg[~customers_above_avg['C_CUSTKEY'].isin(orders_customers['O_CUSTKEY'])]

# Aggregate based on country codes
custsale = customers_no_orders.groupby('CNTRYCODE').agg(NUMCUST=('C_CUSTKEY', 'count'), TOTACCTBAL=('C_ACCTBAL', 'sum')).reset_index()

# Sort by country codes
custsale.sort_values(by='CNTRYCODE', inplace=True)

# Write to CSV
custsale.to_csv('query_output.csv', index=False)
