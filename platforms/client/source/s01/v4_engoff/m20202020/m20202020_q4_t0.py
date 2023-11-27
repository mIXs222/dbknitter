# import statements
import pymysql
import pandas as pd
import direct_redis

# MySQL connection setup
mysql_connection = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)

# Redis connection setup
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query lineitem from mysql
mysql_query = """
SELECT L_ORDERKEY FROM lineitem 
WHERE L_RECEIPTDATE > L_COMMITDATE 
AND L_SHIPDATE BETWEEN '1993-07-01' AND '1993-10-01';
"""

# Execute mysql query
lineitem_df = pd.read_sql_query(mysql_query, mysql_connection)
mysql_connection.close()

# Get orders from Redis as DataFrame
orders_data = redis_connection.get('orders')
orders_df = pd.read_json(orders_data, orient='records')

# Merge two datasets to find matching orders
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')

# Aggregating the count of such orders for each order priority
result_df = merged_df.groupby('O_ORDERPRIORITY').size().reset_index(name='order_count')

# Sort the result by order priority
sorted_result_df = result_df.sort_values('O_ORDERPRIORITY')

# Write the output to a csv file
sorted_result_df.to_csv('query_output.csv', index=False)
