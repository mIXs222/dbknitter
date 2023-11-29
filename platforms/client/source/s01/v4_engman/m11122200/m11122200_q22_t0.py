import pandas as pd
import pymysql
import direct_redis
from datetime import datetime, timedelta

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Redis connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch orders from the last 7 years
seven_years_ago = (datetime.now() - timedelta(days=7*365)).strftime('%Y-%m-%d')
mysql_sql = f"""
SELECT O_CUSTKEY
FROM orders
WHERE O_ORDERDATE >= '{seven_years_ago}'
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_sql)
    recent_orders = cursor.fetchall()

# Create a dataframe for orders
recent_orders_df = pd.DataFrame(recent_orders, columns=['O_CUSTKEY'])
recent_customers = set(recent_orders_df['O_CUSTKEY'])

# Get customer data from Redis
customer_df = pd.read_json(redis_conn.get('customer'), orient='records')

# Process customers that have not placed orders in the last 7 years
customer_df = customer_df[~customer_df['C_CUSTKEY'].isin(recent_customers)]

# Account balance conditions
customer_df = customer_df[customer_df['C_ACCTBAL'] > 0.00]

# Average account balance for people in specific countries
avg_balance_by_country = customer_df.groupby(customer_df['C_PHONE'].str[:2])['C_ACCTBAL'].mean().to_dict()

# Define the country codes to search for
country_codes = ['20', '40', '22', '30', '39', '42', '21']

# Filter for those country codes and account balance conditions
filtered_customers = customer_df[
    (customer_df['C_PHONE'].str[:2].isin(country_codes)) &
    (customer_df['C_ACCTBAL'] > customer_df['C_PHONE'].str[:2].map(avg_balance_by_country))
]

# Perform the final aggregation
output = filtered_customers.groupby(filtered_customers['C_PHONE'].str[:2]).agg(
    CNTRYCODE=('C_PHONE', 'first'),
    Number_of_Customers=('C_CUSTKEY', 'count'),
    Total_Account_Balance=('C_ACCTBAL', 'sum')
).sort_values('CNTRYCODE')[['CNTRYCODE', 'Number_of_Customers', 'Total_Account_Balance']]

# Write to CSV
output.to_csv('query_output.csv', index=False)

# Close connection
mysql_conn.close()
redis_conn.close()
