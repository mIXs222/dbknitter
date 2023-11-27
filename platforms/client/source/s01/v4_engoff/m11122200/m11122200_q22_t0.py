# mysql_redis_query.py
import pymysql
import direct_redis
import pandas as pd
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Query to get customers' details from MySQL
mysql_query = """
SELECT O_CUSTKEY
FROM orders
WHERE O_ORDERDATE < %s
GROUP BY O_CUSTKEY
HAVING COUNT(*) = 0
"""

seven_years_ago = (datetime.datetime.now() - datetime.timedelta(days=7*365)).strftime('%Y-%m-%d')
cursor = mysql_conn.cursor()
cursor.execute(mysql_query, (seven_years_ago,))
result = cursor.fetchall()
cust_keys_with_no_orders = [row[0] for row in result]
cursor.close()
mysql_conn.close()

# Connect to Redis and get customer details
redis_conn = direct_redis.DirectRedis(host='redis', port=6379)
customer_data = redis_conn.get('customer')
customer_df = pd.read_json(customer_data, orient='records')

# Filter customers based on conditions
filtered_customers = customer_df[customer_df['C_CUSTKEY'].isin(cust_keys_with_no_orders)]
filtered_customers = filtered_customers[filtered_customers['C_PHONE'].str[:2].isin(['20', '40', '22', '30', '39', '42', '21'])]
filtered_customers = filtered_customers[filtered_customers['C_ACCTBAL'] > 0.0]

# Group by country code and calculate average balance
grouped_customers = filtered_customers.groupby(filtered_customers['C_PHONE'].str[:2])
final_result = grouped_customers.agg(customer_count=('C_CUSTKEY', 'count'),
                                     average_balance=('C_ACCTBAL', 'mean')).reset_index()
final_result.rename(columns={'C_PHONE': 'country_code'}, inplace=True)

# Write to CSV
final_result.to_csv('query_output.csv', index=False)
