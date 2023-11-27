import pandas as pd
from datetime import datetime, timedelta
import direct_redis

# Connection information
hostname = 'redis'
port = 6379
db_name = '0'

# Connect to Redis using DirectRedis
redis_conn = direct_redis.DirectRedis(host=hostname, port=port, db=db_name)

# Fetch data from Redis
customer_df = pd.read_json(redis_conn.get('customer'))
orders_df = pd.read_json(redis_conn.get('orders'))

# Convert date strings to datetime objects
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filter orders that are older than 7 years
seven_years_ago = datetime.now() - timedelta(days=7*365)
older_orders = orders_df[orders_df['O_ORDERDATE'] < seven_years_ago]

# Customers who have not placed an order for 7 years
customers_without_orders = customer_df[
    ~customer_df['C_CUSTKEY'].isin(older_orders['O_CUSTKEY'])
]

# Filter customers with an average account balance greater than 0.00
potential_customers = customers_without_orders[
    customers_without_orders['C_ACCTBAL'] > 0.00
]

# Define the country codes to filter by
country_codes = ['20', '40', '22', '30', '39', '42', '21']

# Filter customers by the specified country codes
potential_customers['COUNTRY_CODE'] = potential_customers['C_PHONE'].str[:2]
potential_customers_in_countries = potential_customers[
    potential_customers['COUNTRY_CODE'].isin(country_codes)
]

# Group by country code and calculate the number of customers and sum of account balance
output = potential_customers_in_countries.groupby('COUNTRY_CODE').agg(
    Number_of_Customers=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    Sum_of_Account_Balance=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index()

# Write the output to a CSV file
output.to_csv('query_output.csv', index=False)
