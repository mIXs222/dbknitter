import pymysql
import pandas as pd
from datetime import datetime, timedelta
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

with mysql_conn:
    with mysql_conn.cursor() as cursor:
        # Select customers with a specific range of country codes whose account balance is greater than 0
        cursor.execute("""
            SELECT C_CUSTKEY, C_PHONE, C_ACCTBAL
            FROM customer 
            WHERE SUBSTRING(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
            AND C_ACCTBAL > 0.00
        """)
        customer_data = cursor.fetchall()

# Convert to DataFrame
customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_PHONE', 'C_ACCTBAL'])

# Use the DirectRedis library
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve orders table from Redis
orders_df = redis_connection.get('orders')

# Convert to DataFrame if data is present
if orders_df is not None:
    orders_df = pd.DataFrame(orders_df)
else:
    orders_df = pd.DataFrame(columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

# Parse the O_ORDERDATE as datetime
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filter out customers who have not placed orders for 7 years (today - 7 years)
seven_years_ago = datetime.now() - timedelta(days=7*365)
customers_no_orders_7_years = orders_df[orders_df['O_ORDERDATE'] <= seven_years_ago]['O_CUSTKEY'].unique()

# Final DataFrame that fulfills the conditions
final_df = customer_df[~customer_df['C_CUSTKEY'].isin(customers_no_orders_7_years)]

# Country code is the first two characters of c_phone
final_df['COUNTRY_CODE'] = final_df['C_PHONE'].str[:2]

# Group by country code and count unique customers, calculate average account balance
result_df = final_df.groupby('COUNTRY_CODE').agg(num_customers=('C_CUSTKEY', 'nunique'), avg_acct_balance=('C_ACCTBAL', 'mean')).reset_index()

# Write the output to csv
result_df.to_csv('query_output.csv', index=False)
