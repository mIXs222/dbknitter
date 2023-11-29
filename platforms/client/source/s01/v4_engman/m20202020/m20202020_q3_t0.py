import pymysql
import pandas as pd
from datetime import datetime
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query MySQL data
mysql_query = """
SELECT
    l.L_ORDERKEY,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE,
    c.C_MKTSEGMENT
FROM
    lineitem l
JOIN
    customer c ON l.L_ORDERKEY = c.C_CUSTKEY
WHERE
    l.L_SHIPDATE > '1995-03-15'
GROUP BY
    l.L_ORDERKEY
HAVING
    c.C_MKTSEGMENT = 'BUILDING'
"""

lineitem_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db='0')

# Retrieve and process Redis data
orders_str = r.get('orders')
orders_df = pd.read_json(orders_str)

# Combine and process data
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
orders_df_filtered = orders_df[
    (orders_df['O_ORDERDATE'] < datetime(1995, 3, 5))
]

combined_df = pd.merge(
    orders_df_filtered,
    lineitem_df,
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY',
    how='inner'
)

# Select and sort the final output
output_df = combined_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]
output_df = output_df.sort_values(by='REVENUE', ascending=False)

# Write to CSV
output_df.to_csv('query_output.csv', index=False)
