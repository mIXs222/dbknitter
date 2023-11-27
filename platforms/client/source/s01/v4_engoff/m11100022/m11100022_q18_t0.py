import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Retrieve customers from MySQL
mysql_query = """
    SELECT C_NAME, C_CUSTKEY
    FROM customer
"""
customers_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis and retrieve orders and lineitem as Pandas DataFrame
redis_conn = DirectRedis(host='redis', port=6379, db=0)
orders_str = redis_conn.get('orders')
lineitem_str = redis_conn.get('lineitem')

orders_df = pd.read_json(orders_str)
lineitem_df = pd.read_json(lineitem_str)

# Filter lineitem for large quantity orders (quantity > 300)
large_lineitem_df = lineitem_df[lineitem_df['L_QUANTITY'] > 300]

# Merge the orders with the large lineitem DataFrame on order key
large_orders_df = orders_df.merge(large_lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Selecting only necessary columns for final query output
result_df = large_orders_df[['O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Merge the result with the customers DataFrame on customer key
final_result_df = pd.merge(customers_df, result_df, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Selecting the specific columns for the output
output_df = final_result_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Writing to output CSV file
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
