import pymysql
import pandas as pd
from datetime import datetime
import direct_redis

# MySQL connection details
mysql_connection_details = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Establish a connection to MySQL database
mysql_connection = pymysql.connect(**mysql_connection_details)
mysql_cursor = mysql_connection.cursor()

# Query to retrieve orders within the specific timeframe
mysql_query = """
SELECT O_ORDERPRIORITY, O_ORDERKEY
FROM orders
WHERE O_ORDERDATE >= '1993-07-01' and O_ORDERDATE < '1993-10-01';
"""
mysql_cursor.execute(mysql_query)

# Get order data
orders_data = mysql_cursor.fetchall()
orders_df = pd.DataFrame(orders_data, columns=['O_ORDERPRIORITY', 'O_ORDERKEY'])

# Close MySQL connection
mysql_cursor.close()
mysql_connection.close()

# Redis connection details
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis and decode bytes to string
lineitem_data = eval(redis_connection.get('lineitem').decode())

# Create a DataFrame for lineitem data
lineitem_df = pd.DataFrame(lineitem_data)

# Join tables on order key and filter based on conditions
result = (
    orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .query("L_COMMITDATE < L_RECEIPTDATE")
    .groupby("O_ORDERPRIORITY", as_index=False)
    .agg(OrderCount=('O_ORDERKEY', 'nunique'))
    .sort_values('O_ORDERPRIORITY', ascending=True)
)

# Write results to CSV
result.to_csv('query_output.csv', index=False)
