import pymysql
import pandas as pd
import direct_redis
from datetime import datetime
from sqlalchemy import create_engine

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Execute MySQL query
mysql_query = """
SELECT O_ORDERPRIORITY, COUNT(*) AS order_count
FROM orders
WHERE O_ORDERDATE >= '1993-07-01'
AND O_ORDERDATE < '1993-10-01'
GROUP BY O_ORDERPRIORITY
ORDER BY O_ORDERPRIORITY;
"""
orders_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis and get lineitem DataFrame
redis_engine = direct_redis.DirectRedis(host='redis', port=6379, db=0)
lineitem_df = redis_engine.get('lineitem')
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
late_lineitems = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']]

# Merge dataframes to filter orders with late lineitems
orders_with_late_lineitems = pd.merge(
    orders_df, late_lineitems,
    left_on='O_ORDERKEY', right_on='L_ORDERKEY'
)
orders_with_late_lineitems = orders_with_late_lineitems.groupby('O_ORDERPRIORITY').size().reset_index(name='order_count')

# Save query results to CSV
orders_with_late_lineitems.to_csv('query_output.csv', index=False)
