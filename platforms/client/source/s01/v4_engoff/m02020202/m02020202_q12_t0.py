import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connection info for MySQL
mysql_connection_info = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql'
}

# Connection to MySQL
mysql_conn = pymysql.connect(**mysql_connection_info)
cursor = mysql_conn.cursor()

# Fetch orders data from MySQL
sql = """
SELECT O_ORDERKEY, O_ORDERPRIORITY
FROM orders
WHERE O_ORDERPRIORITY = 'URGENT' OR O_ORDERPRIORITY = 'HIGH'
"""
cursor.execute(sql)
orders_data = cursor.fetchall()
orders_df = pd.DataFrame(orders_data, columns=['O_ORDERKEY', 'O_ORDERPRIORITY'])
cursor.close()
mysql_conn.close()

# Connection info for Redis
redis_connection_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connect to Redis
redis_conn = DirectRedis(**redis_connection_info)

# Fetch lineitem data from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Filter lineitem data
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (lineitem_df['L_RECEIPTDATE'] >= pd.Timestamp('1994-01-01')) &
    (lineitem_df['L_RECEIPTDATE'] <= pd.Timestamp('1995-01-01')) &
    (lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE'])
]

# Merge the filtered data
merged_df = pd.merge(
    filtered_lineitem_df,
    orders_df,
    how='inner',
    left_on='L_ORDERKEY',
    right_on='O_ORDERKEY'
)

# Get the count of late lineitems by SHIPMODE and ORDERPRIORITY
result_df = merged_df.groupby(['L_SHIPMODE', 'O_ORDERPRIORITY']).size().reset_index(name='COUNT')

# Write result to file
result_df.to_csv('query_output.csv', index=False)
