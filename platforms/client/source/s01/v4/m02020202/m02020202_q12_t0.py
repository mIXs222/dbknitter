import pandas as pd
import pymysql
import direct_redis

# Connect to mysql
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Get orders table from mysql
mysql_query = """
SELECT O_ORDERKEY, O_ORDERPRIORITY
FROM orders
WHERE (O_ORDERPRIORITY = '1-URGENT' OR O_ORDERPRIORITY = '2-HIGH')
      OR (O_ORDERPRIORITY <> '1-URGENT' AND O_ORDERPRIORITY <> '2-HIGH')
"""
orders_df = pd.read_sql(mysql_query, mysql_connection)
mysql_connection.close()

# Connect to redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get lineitem from redis
lineitem_df = redis_connection.get('lineitem')

# Convert lineitem to dataframe
lineitem_df = pd.DataFrame.from_records(lineitem_df)

# Filter lineitem dataframe as per the query requirements
lineitem_filtered = lineitem_df[
    (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']) &
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_RECEIPTDATE'] >= '1994-01-01') &
    (lineitem_df['L_RECEIPTDATE'] < '1995-01-01')
]

# Merge orders and lineitem dataframes on O_ORDERKEY == L_ORDERKEY
merged_df = pd.merge(orders_df, lineitem_filtered, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')

# Compute high_line_count and low_line_count
merged_df['HIGH_LINE_COUNT'] = (merged_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])).astype(int)
merged_df['LOW_LINE_COUNT'] = (~merged_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])).astype(int)

# Group by L_SHIPMODE and calculate sum
result_df = merged_df.groupby('L_SHIPMODE').agg({
    'HIGH_LINE_COUNT': 'sum',
    'LOW_LINE_COUNT': 'sum'
}).reset_index()

# Sort the result dataframe by L_SHIPMODE
result_df = result_df.sort_values('L_SHIPMODE')

# Save the result to CSV
result_df.to_csv('query_output.csv', index=False)
