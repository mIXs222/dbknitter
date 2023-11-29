import pymysql
import pandas as pd
import direct_redis

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Get orders from MySQL with the conditions specified
mysql_query = """
SELECT O_ORDERKEY, O_ORDERPRIORITY
FROM orders
WHERE O_ORDERDATE >= '1993-07-01'
AND O_ORDERDATE < '1993-10-01';
"""
orders_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Redis Connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get lineitem from Redis
lineitem_df = redis_conn.get('lineitem')

# Convert lineitem_df from JSON to pandas DataFrame
lineitem_df = pd.read_json(lineitem_df)

# Merge and filter necessary data
merged_df = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
late_orders_df = merged_df[merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE']]

# Aggregate the results
final_df = late_orders_df.groupby('O_ORDERPRIORITY') \
    .size() \
    .reset_index(name='ORDER_COUNT') \
    .sort_values(by='O_ORDERPRIORITY')

# Save to CSV
final_df.to_csv('query_output.csv', index=False)
