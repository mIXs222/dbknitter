import pandas as pd
from direct_redis import DirectRedis

# Create a connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Read the customer and orders tables into Pandas DataFrames
df_customer = pd.read_json(redis_client.get('customer'))
df_orders = pd.read_json(redis_client.get('orders'))

# Prepare the data according to the query conditions
# Extract the country code from the phone number
df_customer['CNTRYCODE'] = df_customer['C_PHONE'].str.slice(0, 2)
# Convert O_ORDERDATE to DateTime
df_orders['O_ORDERDATE'] = pd.to_datetime(df_orders['O_ORDERDATE'])
# Filter the customer DataFrame for customers from the specified country codes
country_codes = ['20', '40', '22', '30', '39', '42', '21']
df_customer = df_customer[df_customer['CNTRYCODE'].isin(country_codes)]

# Calculate the average account balance for accounts greater than 0.00
avg_account_balance = df_customer[df_customer['C_ACCTBAL'] > 0.00]['C_ACCTBAL'].mean()

# Filter customers who have not placed orders for 7 years
seven_years_ago = pd.Timestamp.now() - pd.DateOffset(years=7)
df_customer_with_no_recent_orders = df_customer[~df_customer['C_CUSTKEY'].isin(df_orders[df_orders['O_ORDERDATE'] >= seven_years_ago]['O_CUSTKEY'])]

# Filter customers whose account balance is greater than the average account balance
df_filtered_customers = df_customer_with_no_recent_orders[df_customer_with_no_recent_orders['C_ACCTBAL'] > avg_account_balance]

# Group by country code and calculate the required statistics
result = df_filtered_customers.groupby('CNTRYCODE').agg(
    num_customers=('C_CUSTKEY', 'count'),
    total_account_balance=('C_ACCTBAL', 'sum')
).reset_index()

# Sort the results by CNTRYCODE
result = result.sort_values('CNTRYCODE')

# Write the output to query_output.csv
result.to_csv('query_output.csv', index=False)

