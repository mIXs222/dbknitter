import redis
import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis database
r = DirectRedis(host='redis', port=6379, db=0)

# Read data from Redis
customer_df = pd.read_json(r.get('customer'), orient='records')
orders_df = pd.read_json(r.get('orders'), orient='records')

# Extract country codes from phone numbers
customer_df['CNTRYCODE'] = customer_df['C_PHONE'].apply(lambda x: x[:2])

# Filter customers by countries in the specific list
countries = ['20', '40', '22', '30', '39', '42', '21']
customer_df = customer_df[customer_df['CNTRYCODE'].isin(countries)]

# Calculate the average account balance for customers with positive balances
average_positive_balance = customer_df[customer_df['C_ACCTBAL'] > 0]['C_ACCTBAL'].mean()

# Include only customers with account balance greater than the average
customer_df = customer_df[customer_df['C_ACCTBAL'] > average_positive_balance]

# Exclude customers who have placed orders
customers_with_orders = set(orders_df['O_CUSTKEY'])
customer_df = customer_df[~customer_df['C_CUSTKEY'].isin(customers_with_orders)]

# Group by country code and summarize the number of customers and total account balances
result_df = customer_df.groupby('CNTRYCODE').agg(NUMCUST=('C_CUSTKEY', 'count'), TOTACCTBAL=('C_ACCTBAL', 'sum')).sort_values('CNTRYCODE').reset_index()

# Write the results to a CSV file
result_df.to_csv('query_output.csv', index=False)
