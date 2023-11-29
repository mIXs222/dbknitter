import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connection information
hostname = 'redis'
port = 6379
dbname = 0

# Connect to Redis using DirectRedis
redis_client = DirectRedis(host=hostname, port=port, db=dbname)

# Get data from Redis
customer_df = pd.read_json(redis_client.get('customer'))
orders_df = pd.read_json(redis_client.get('orders'))

# Filter customers by country codes
valid_country_codes = ['20', '40', '22', '30', '39', '42', '21']
customer_df = customer_df[customer_df['C_PHONE'].str[:2].isin(valid_country_codes)]

# Calculate average account balance where balance is larger than 0 for each country code
average_balance = customer_df[customer_df['C_ACCTBAL'] > 0].groupby(customer_df['C_PHONE'].str[:2])['C_ACCTBAL'].mean()

# Get current date and calculate the date 7 years ago
current_date = datetime.datetime.now()
date_7_years_ago = current_date - datetime.timedelta(days=365*7)

# Find customers who have not placed any orders for 7 years
customers_no_orders_7yrs = customer_df[~customer_df['C_CUSTKEY'].isin(orders_df['O_CUSTKEY']) &
                                        (pd.to_datetime(customer_df['C_ACCTBAL']) > date_7_years_ago)]

# Create a dataframe to store results
results = pd.merge(customers_no_orders_7yrs, average_balance.rename('AVG_ACCTBAL'), left_on=customer_df['C_PHONE'].str[:2], right_index=True)

# Filter by customers with account balance greater than the average in their country
results = results[results['C_ACCTBAL'] > results['AVG_ACCTBAL']]

# Group by country code, count customers and sum account balances
final_results = results.groupby(results['C_PHONE'].str[:2]).agg(
    num_customers=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    total_acct_balance=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index()

# Rename country code column
final_results.rename(columns={'C_PHONE': 'CNTRYCODE'}, inplace=True)

# Order by country code
final_results = final_results.sort_values(by='CNTRYCODE')

# Write to CSV
final_results.to_csv('query_output.csv', index=False)
