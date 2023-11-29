import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime, timedelta

# Establish a connection to the Redis database
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Read DataFrames from Redis using the DirectRedis connection
customers_df = pd.read_json(redis_conn.get('customer'), orient='records')
orders_df = pd.read_json(redis_conn.get('orders'), orient='records')

# Convert C_PHONE to country code by taking the first two characters
customers_df['CNTRYCODE'] = customers_df['C_PHONE'].str.slice(0, 2)

# Filter country codes according to the requirement
filtered_customers_df = customers_df[customers_df['CNTRYCODE'].isin(['20', '40', '22', '30', '39', '42', '21'])]

# Get the current date and calculate the date 7 years ago
current_date = datetime.now()
seven_years_ago = current_date - timedelta(days=7*365)

# Convert O_ORDERDATE to datetime
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Get customers who have not placed orders for 7 years and have a positive account balance
orders_7_years = orders_df[orders_df['O_ORDERDATE'] > seven_years_ago]
customers_no_order_7_years = filtered_customers_df[~filtered_customers_df['C_CUSTKEY'].isin(orders_7_years['O_CUSTKEY'])]
customers_positive_balance = customers_no_order_7_years[customers_no_order_7_years['C_ACCTBAL'] > 0]

# Get the average account balance of people with an account balance greater than 0.00 for each country code
avg_acct_balance_per_country = customers_positive_balance.groupby('CNTRYCODE')['C_ACCTBAL'].mean().reset_index(name='AVG_ACCTBAL')

# Merge customers with average account balance per country to filter customers with account balance greater than the average
result = pd.merge(customers_positive_balance, avg_acct_balance_per_country, on='CNTRYCODE')
result_filtered = result[result['C_ACCTBAL'] > result['AVG_ACCTBAL']]

# Group by CNTRYCODE and count the number of such customers and sum of their account balances
final_result = result_filtered.groupby('CNTRYCODE').agg(
    NUM_CUSTOMERS=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTAL_ACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum'),
).reset_index()

# Sort by CNTRYCODE ascending
final_result_sorted = final_result.sort_values(by='CNTRYCODE')

# Write the final result to a CSV file
final_result_sorted.to_csv('query_output.csv', index=False)
