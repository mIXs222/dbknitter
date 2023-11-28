# query.py

import pymysql
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Query lineitem table from MySQL
mysql_query = """
SELECT
    L_ORDERKEY, L_SHIPMODE
FROM
    lineitem
WHERE
    L_SHIPMODE IN ('MAIL', 'SHIP')
    AND L_COMMITDATE < L_RECEIPTDATE
    AND L_SHIPDATE < L_COMMITDATE
    AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1994-12-31'
"""
lineitem_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get orders table from Redis
orders_df = pd.read_json(redis_conn.get('orders'))

# Process orders to identify priority
orders_df['PRIORITY'] = ['HIGH' if x in ['1-URGENT', '2-HIGH'] else 'LOW' for x in orders_df['O_ORDERPRIORITY']]

# Merge dataframes on order key
merged_df = lineitem_df.merge(orders_df, how="inner", left_on="L_ORDERKEY", right_on="O_ORDERKEY")

# Group the merged data by shipping mode and priority
grouped = merged_df.groupby(['L_SHIPMODE', 'PRIORITY'])

# Calculate counts
result = grouped['L_ORDERKEY'].count().unstack(fill_value=0).rename(columns={'HIGH':'HIGH_LINE_COUNT', 'LOW':'LOW_LINE_COUNT'})

# Sort the result
result.sort_index(inplace=True)

# Write to csv
result.to_csv('query_output.csv')
