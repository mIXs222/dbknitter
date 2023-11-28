import pandas as pd
from direct_redis import DirectRedis
import csv

# Setting up the connection to the Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetching tables as Pandas DataFrames using the redis client
customer_df = pd.read_json(redis_client.get('customer'), orient='records')
orders_df = pd.read_json(redis_client.get('orders'), orient='records')

# Performing the analysis and computations
# Extract the country code from the phone number
customer_df['CNTRYCODE'] = customer_df['C_PHONE'].str.slice(0, 2)

# Filter for specified country codes and positive account balances
specified_country_codes = ['20', '40', '22', '30', '39', '42', '21']
customers_positive_balances = customer_df[customer_df['C_ACCTBAL'] > 0]
average_balances = customers_positive_balances.groupby('CNTRYCODE')['C_ACCTBAL'].mean().to_dict()
customer_df = customer_df[customer_df['CNTRYCODE'].isin(specified_country_codes)]
customer_df['AVG_C_ACCTBAL'] = customer_df['CNTRYCODE'].map(average_balances)

# Include only customers with balances greater than the average balance for the country
filtered_customers = customer_df[customer_df['C_ACCTBAL'] > customer_df['AVG_C_ACCTBAL']]

# Exclude customers who have placed orders
customers_with_orders = orders_df['O_CUSTKEY'].unique()
filtered_customers = filtered_customers[~filtered_customers['C_CUSTKEY'].isin(customers_with_orders)]

# Final aggregation
result = filtered_customers.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='size'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index().sort_values('CNTRYCODE')

# Write to CSV
result.to_csv('query_output.csv', index=False)
