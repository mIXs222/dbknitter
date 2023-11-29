import csv
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Retrieve orders data from MySQL
orders_query = """
    SELECT O_CUSTKEY, COUNT(*) as order_count
    FROM orders
    WHERE O_ORDERSTATUS NOT IN ('P', 'D')
    AND O_COMMENT NOT LIKE '%pending%deposits%'
    GROUP BY O_CUSTKEY
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(orders_query)
    orders_result = cursor.fetchall()

# Put orders result into DataFrame
orders_df = pd.DataFrame(orders_result, columns=['C_CUSTKEY', 'order_count'])

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve customer data from Redis
customer_df = pd.read_json(redis_conn.get('customer'), orient='records')

# Merge orders data with customer data on customer key
merged_df = pd.merge(customer_df, orders_df, on='C_CUSTKEY', how='left').fillna(0)

# Group by order count to get distribution of customers
distribution_df = merged_df.groupby('order_count').size().reset_index(name='customer_count')

# Write the query result to a csv file
distribution_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
