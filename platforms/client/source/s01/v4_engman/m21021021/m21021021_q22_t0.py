# query_code.py

import pymysql
import pandas as pd
from datetime import datetime, timedelta
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query customers from MySQL
customer_query = """
SELECT C_CUSTKEY, C_PHONE, C_ACCTBAL
FROM customer
WHERE SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
AND C_ACCTBAL > 0.00;
"""
mysql_cursor = mysql_connection.cursor()
mysql_cursor.execute(customer_query)
customer_data = mysql_cursor.fetchall()
customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_PHONE', 'C_ACCTBAL'])

# Get orders from Redis
orders_df = pd.DataFrame(redis_connection.get('orders'))

# Pre-process the orders data
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
seven_years_ago = datetime.now() - timedelta(days=7*365)
orders_df = orders_df[orders_df['O_ORDERDATE'] < seven_years_ago]

# Use a left join to merge customer and orders data and filter non-ordered customers
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')
non_ordered_df = merged_df[pd.isnull(merged_df['O_ORDERKEY'])]

# Calculate average account balance for customers with account balance greater than 0
avg_acct_bal = customer_df[customer_df['C_ACCTBAL'] > 0]['C_ACCTBAL'].mean()

# Filter customers with balance greater than the average
potential_customers = non_ordered_df[non_ordered_df['C_ACCTBAL'] > avg_acct_bal]

# Extract country code and aggregate the results
potential_customers['CNTRYCODE'] = potential_customers['C_PHONE'].str.slice(0, 2)
result = potential_customers.groupby('CNTRYCODE').agg(
    num_customers=('C_CUSTKEY', 'count'),
    total_acct_balance=('C_ACCTBAL', 'sum')
).reset_index()

# Save the results to CSV
result.to_csv('query_output.csv', index=False)

# clean up connections
mysql_cursor.close()
mysql_connection.close()
