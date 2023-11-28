import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute query for the orders table in MySQL
query_mysql = """
SELECT O_ORDERKEY, O_ORDERPRIORITY
FROM orders
WHERE O_ORDERPRIORITY IN ('1-URGENT', '2-HIGH') OR O_ORDERPRIORITY NOT IN ('1-URGENT', '2-HIGH')
"""
orders_df = pd.read_sql(query_mysql, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379)

# Read lineitem DataFrame from Redis
lineitem_df = redis_conn.get('lineitem')  # Assuming this returns a DataFrame

# Process the `lineitem` DataFrame to meet the query's criteria
lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']) &
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_RECEIPTDATE'] >= '1994-01-01') &
    (lineitem_df['L_RECEIPTDATE'] <= '1994-12-31')
]

# Merge dataframes
df_merged = pd.merge(lineitem_df, orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Calculate the counts
df_result = df_merged.groupby(['L_SHIPMODE', 'O_ORDERPRIORITY']).size().unstack(fill_value=0)
df_result['HIGH_LINE_COUNT'] = df_result['1-URGENT'] + df_result['2-HIGH']
df_result['LOW_LINE_COUNT'] = df_result.sum(axis=1) - df_result['HIGH_LINE_COUNT']
final_result = df_result[['HIGH_LINE_COUNT', 'LOW_LINE_COUNT']].sort_index()

# Write to CSV
final_result.to_csv('query_output.csv')
