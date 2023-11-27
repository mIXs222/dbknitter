import pymysql
import pandas as pd
import direct_redis
import csv


# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Retrieve data from MySQL
query_mysql = """
SELECT
    L_ORDERKEY,
    L_COMMITDATE,
    L_RECEIPTDATE
FROM
    lineitem
WHERE
    L_COMMITDATE < L_RECEIPTDATE
"""

with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute(query_mysql)
        lineitem_data = cursor.fetchall()

# Transform MySQL data to DataFrame
lineitem_df = pd.DataFrame(lineitem_data, columns=['L_ORDERKEY', 'L_COMMITDATE', 'L_RECEIPTDATE'])

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis
orders_data = r.get('orders')
orders_df = pd.read_csv(orders_data, index_col=False) if orders_data else pd.DataFrame()

# Filter orders DataFrame
filtered_orders_df = orders_df[(orders_df['O_ORDERDATE'] >= '1993-07-01') & 
                               (orders_df['O_ORDERDATE'] < '1993-10-01')]

# Join DataFrames
merged_df = pd.merge(filtered_orders_df, lineitem_df, 
                     left_on='O_ORDERKEY', 
                     right_on='L_ORDERKEY', 
                     how='inner')

# Group by O_ORDERPRIORITY
result_df = merged_df.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

# Sort by O_ORDERPRIORITY
result_df = result_df.sort_values(by='O_ORDERPRIORITY')

# Write result to CSV
result_df.to_csv('query_output.csv', index=False)
