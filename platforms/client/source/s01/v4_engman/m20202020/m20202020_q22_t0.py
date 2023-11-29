import pymysql
import redis
import pandas as pd
from io import StringIO
import direct_redis

# Establish connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to the Redis server
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the average account balance of people whose account balance is larger than 0.00
with mysql_conn.cursor() as cursor:
    avg_query = """
    SELECT AVG(C_ACCTBAL)
    FROM customer
    WHERE C_ACCTBAL > 0.00;
    """
    cursor.execute(avg_query)
    avg_balance = cursor.fetchone()[0]

# Query to fetch customers having account balance greater than the average
cust_query = """
SELECT LEFT(C_PHONE, 2) AS CNTRYCODE, C_CUSTKEY, C_ACCTBAL
FROM customer
WHERE C_ACCTBAL > %s
AND LEFT(C_PHONE, 2) IN ('20', '40', '22', '30', '39', '42', '21');
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(cust_query, (avg_balance,))
    customer_data = cursor.fetchall()

# Convert customer data into a DataFrame
customer_df = pd.DataFrame(customer_data, columns=['CNTRYCODE', 'C_CUSTKEY', 'C_ACCTBAL'])

# Get orders data from Redis and convert to DataFrame
orders_data = redis_conn.get('orders')
orders_df = pd.read_csv(StringIO(orders_data))

# Convert the O_ORDERDATE to datetime format
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filter orders that are more than 7 years old
seven_years_ago = pd.Timestamp.now() - pd.DateOffset(years=7)
old_orders_df = orders_df[orders_df['O_ORDERDATE'] <= seven_years_ago]

# Get the unique customer keys from old orders
old_cust_keys = old_orders_df['O_CUSTKEY'].unique()

# Filter out those customers from the customer_df
customer_df = customer_df[~customer_df['C_CUSTKEY'].isin(old_cust_keys)]

# Group by CNTRYCODE, count customers, and sum the account balances
result = customer_df.groupby('CNTRYCODE').agg(
    count=('C_CUSTKEY', 'size'),
    total_balance=('C_ACCTBAL', 'sum')
).reset_index()

# Sort the results by CNTRYCODE ascending
result = result.sort_values(by='CNTRYCODE', ascending=True)

# Write the query result to query_output.csv
result.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
redis_conn.close()
