import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# SQL query to get relevant lineitem data
mysql_query = """
SELECT L_ORDERKEY
FROM lineitem
WHERE L_COMMITDATE < L_RECEIPTDATE
"""

lineitems = pd.read_sql(mysql_query, mysql_connection)

# Close the MySQL connection
mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get orders data from Redis
orders_df_str = redis_connection.get('orders')
orders_df = pd.read_json(orders_df_str)

# Filter orders by date range
orders_df = orders_df[
    (orders_df['O_ORDERDATE'] >= '1993-07-01') &
    (orders_df['O_ORDERDATE'] < '1993-10-01')
]

# Filter orders that exist in the lineitem data
filtered_orders = orders_df[orders_df['O_ORDERKEY'].isin(lineitems['L_ORDERKEY'])]

# Group by O_ORDERPRIORITY
result = filtered_orders.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

# Sort the results
result = result.sort_values('O_ORDERPRIORITY')

# Write to CSV
result.to_csv('query_output.csv', index=False)
