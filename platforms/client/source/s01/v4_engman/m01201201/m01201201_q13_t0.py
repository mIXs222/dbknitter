# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

try:
    with mysql_conn.cursor() as cursor:
        # MySQL query to get orders excluding 'pending' and 'deposits'
        mysql_query = """
        SELECT O_CUSTKEY, COUNT(*) as order_count
        FROM orders
        WHERE O_ORDERSTATUS NOT IN ('P', 'D')
        AND O_COMMENT NOT LIKE '%pending%deposits%'
        GROUP BY O_CUSTKEY
        """
        cursor.execute(mysql_query)
        orders_result = cursor.fetchall()
finally:
    mysql_conn.close()

# Convert MySQL data to Pandas DataFrame
orders_df = pd.DataFrame(orders_result, columns=['C_CUSTKEY', 'order_count'])

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get 'customer' table data from Redis
customer_json = redis_conn.get('customer')
customer_df = pd.read_json(customer_json, orient='records')

# Merge Redis and MySQL data on 'C_CUSTKEY'
merged_df = pd.merge(customer_df, orders_df, on='C_CUSTKEY', how='inner')

# Determine the distribution of customers by the number of orders
distribution_df = merged_df.groupby('order_count').size().reset_index(name='customer_count')

# Write output to a CSV file
distribution_df.to_csv('query_output.csv', index=False)
