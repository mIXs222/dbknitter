import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to the MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# MySQL query to get the relevant lineitem data
lineitem_query = """
SELECT L_SHIPMODE, L_LINENUMBER, L_RECEIPTDATE, L_COMMITDATE, L_SHIPDATE, L_ORDERKEY
FROM lineitem
WHERE L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
  AND L_RECEIPTDATE > L_COMMITDATE
  AND L_SHIPMODE IN ('MAIL', 'SHIP')
  AND L_SHIPDATE < L_COMMITDATE
"""

lineitem_data = pd.read_sql(lineitem_query, mysql_conn)
mysql_conn.close()

# Connect to Redis database
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get the orders data from Redis
orders_df = pd.read_msgpack(redis_conn.get('orders'))

# Combining data
merged_df = pd.merge(lineitem_data, orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Filter by priority and calculate counts
merged_df['HIGH_PRIORITY'] = merged_df['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH']).astype(int)
merged_df['LOW_PRIORITY'] = (~merged_df['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH'])).astype(int)

result = merged_df.groupby('L_SHIPMODE').agg(
    High_Priority_Count=('HIGH_PRIORITY', 'sum'),
    Low_Priority_Count=('LOW_PRIORITY', 'sum'),
).reset_index()

result.sort_values('L_SHIPMODE', ascending=True, inplace=True)

# Write the results to a CSV file
result.to_csv('query_output.csv', index=False)
