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

# Query the orders table in mysql
orders_query = """
SELECT O_ORDERKEY, O_ORDERPRIORITY
FROM orders
WHERE O_ORDERPRIORITY = '1-URGENT' OR O_ORDERPRIORITY = '2-HIGH'
"""
orders_df = pd.read_sql(orders_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get the lineitem dataframe from Redis
lineitem_df = redis_conn.get('lineitem')
lineitem_df = pd.DataFrame(lineitem_df)

# Keep only necessary columns and rows from the lineitem dataframe
lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_RECEIPTDATE'].between('1994-01-01', '1995-01-01')) &
    (lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE'])
]

# Merge the dataframes on O_ORDERKEY and L_ORDERKEY
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Create the result dataframe from the merged dataframe
result_df = merged_df.groupby('L_SHIPMODE')['L_ORDERKEY'].agg(
    high_priority_count=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: (x.isin(['1-URGENT', '2-HIGH'])).sum()),
    low_priority_count=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: (~x.isin(['1-URGENT', '2-HIGH'])).sum())
).reset_index()

# Save result to CSV
result_df.to_csv('query_output.csv', index=False)
