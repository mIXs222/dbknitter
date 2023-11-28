# python_code.py
import pymysql
import pandas as pd
import direct_redis
from datetime import datetime

# Connect to MySQL and fetch data from the 'orders' table
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
query = """
SELECT O_ORDERKEY, O_ORDERPRIORITY
FROM orders
WHERE O_ORDERPRIORITY IN ('1-URGENT', '2-HIGH') OR O_ORDERPRIORITY NOT IN ('1-URGENT', '2-HIGH')
"""
orders_df = pd.read_sql(query, mysql_conn)
orders_df.columns = orders_df.columns.str.lower()
mysql_conn.close()

# Connect to Redis and fetch data from the 'lineitem' table
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Filter the lineitem dataframe to match the criteria
lineitem_df = lineitem_df[
    (lineitem_df['l_shipmode'].isin(['MAIL', 'SHIP'])) &
    (lineitem_df['l_commitdate'] < lineitem_df['l_receiptdate']) &
    (lineitem_df['l_shipdate'] < lineitem_df['l_commitdate']) &
    (lineitem_df['l_receiptdate'] >= datetime(1994, 1, 1)) &
    (lineitem_df['l_receiptdate'] <= datetime(1994, 12, 31))
]

# Merge two dataframes based on orderkey
merged_df = pd.merge(orders_df, lineitem_df, left_on='o_orderkey', right_on='l_orderkey')

# Calculate the counts
result_df = merged_df.groupby(['l_shipmode', 'o_orderpriority'])['l_orderkey'].count().reset_index()
result_df.columns = ['SHIPMODE', 'ORDERPRIORITY', 'LINE_COUNT']

# Get the High and Low priority line counts
high_df = result_df[result_df['ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])]
low_df = result_df[~result_df['ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])]

# Rename columns
high_df = high_df.rename(columns={'LINE_COUNT': 'HIGH_LINE_COUNT'})
low_df = low_df.rename(columns={'LINE_COUNT': 'LOW_LINE_COUNT'})

# Merge High and Low priority line counts based on shipmode
final_df = pd.merge(high_df[['SHIPMODE', 'HIGH_LINE_COUNT']], low_df[['SHIPMODE', 'LOW_LINE_COUNT']], on='SHIPMODE', how='outer')

# Fill NaN values with 0 and sort
final_df = final_df.fillna(0)
final_df = final_df.sort_values('SHIPMODE')

# Write to CSV
final_df.to_csv('query_output.csv', index=False)
