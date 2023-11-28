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

# Query to get orders with specific date range from MySQL
mysql_query = """
SELECT O_ORDERKEY, O_ORDERPRIORITY FROM orders
WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE <= '1993-10-01'
"""

# DataFrame from MySQL
df_mysql = pd.read_sql_query(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve lineitem DataFrame from Redis
lineitem_data = redis_conn.get('lineitem')
# deserialize the json object to pandas dataframe
df_redis = pd.read_json(lineitem_data)

# Process the data
df_redis = df_redis[df_redis['L_COMMITDATE'] < df_redis['L_RECEIPTDATE']]
order_keys = df_redis['L_ORDERKEY'].unique()
df_mysql_filtered = df_mysql[df_mysql['O_ORDERKEY'].isin(order_keys)]
order_priority_counts = df_mysql_filtered['O_ORDERPRIORITY'].value_counts().sort_index()

# Write to CSV
order_priority_counts.to_csv('query_output.csv', header=True)
