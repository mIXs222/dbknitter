# File: execute_query.py

import pymysql
import direct_redis
import pandas as pd
from datetime import datetime, timedelta

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Fetch orders placed in the last 7 years from MySQL
seven_years_ago = datetime.now() - timedelta(days=7 * 365)
with mysql_conn.cursor() as cursor:
    query_orders = """
    SELECT O_CUSTKEY
    FROM orders
    WHERE O_ORDERDATE >= %s
    """
    cursor.execute(query_orders, (seven_years_ago,))
    active_customers = set(ck[0] for ck in cursor.fetchall())

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch customer data from Redis
customer_data = pd.read_msgpack(redis_connection.get('customer'))

mysql_conn.close()

# Filter customers with specific country codes and positive account balances
country_codes = ['20', '40', '22', '30', '39', '42', '21']
filtered_customers = customer_data[
    (customer_data['C_PHONE'].str[:2].isin(country_codes)) &
    (customer_data['C_ACCTBAL'] > 0.0) &
    (~customer_data['C_CUSTKEY'].isin(active_customers))
]

# Calculate counts and average balances by country code
result = (
    filtered_customers
    .groupby(customer_data['C_PHONE'].str[:2].rename('COUNTRY_CODE'))
    .agg(CUSTOMER_COUNT=('C_CUSTKEY', 'count'), AVERAGE_BALANCE=('C_ACCTBAL', 'mean'))
    .reset_index()
)

# Write result to a CSV file
result.to_csv('query_output.csv', index=False)
