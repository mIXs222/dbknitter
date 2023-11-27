# code.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime, timedelta

# Establish connection to the MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Query to check for customers in MySQL 'orders' table who have not placed orders for the last 7 years
seven_years_ago = (datetime.now() - timedelta(days=7*365)).strftime('%Y-%m-%d')
sql_query = """
SELECT O_CUSTKEY FROM orders WHERE O_ORDERDATE < %s
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(sql_query, (seven_years_ago,))
    custkeys_with_old_orders = [row[0] for row in cursor.fetchall()]

# Close the connection
mysql_conn.close()

# Establish connection to the Redis database
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch 'customer' table stored in Redis
customer_data = redis_conn.get('customer')
df_customer = pd.read_json(customer_data)

# Filter for specific country codes based on the phone number and account balance
country_codes = ['20', '40', '22', '30', '39', '42', '21']
df_filtered_customers = df_customer[(df_customer['C_PHONE'].str.slice(0, 2).isin(country_codes)) &
                                    (df_customer['C_ACCTBAL'] > 0.00) &
                                    (~df_customer['C_CUSTKEY'].isin(custkeys_with_old_orders))]

# Aggregate required information
result = df_filtered_customers.groupby(df_customer['C_PHONE'].str.slice(0, 2)).agg(
    customer_count=('C_CUSTKEY', 'count'),
    avg_balance=('C_ACCTBAL', 'mean')
).reset_index()

# Write the result to CSV file
result.to_csv('query_output.csv', index=False)
