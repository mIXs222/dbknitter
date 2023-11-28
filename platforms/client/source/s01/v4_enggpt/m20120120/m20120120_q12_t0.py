import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Query to get the lineitem data from mysql
lineitem_query = """
SELECT L_ORDERKEY, L_SHIPMODE
FROM lineitem
WHERE L_SHIPMODE IN ('MAIL', 'SHIP')
  AND L_COMMITDATE < L_RECEIPTDATE
  AND L_SHIPDATE < L_COMMITDATE
  AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1994-12-31'
"""
mysql_cursor.execute(lineitem_query)
lineitem_data = mysql_cursor.fetchall()

# Dataframe for lineitem data
df_lineitem = pd.DataFrame(lineitem_data, columns=['L_ORDERKEY', 'L_SHIPMODE'])
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the orders data from redis
orders_data = redis_conn.get('orders')
df_orders = pd.read_csv(pd.compat.StringIO(orders_data.decode('utf-8')))

# Merge dataframes on order key
df_merged = pd.merge(df_lineitem, df_orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Filter high-priority and low-priority orders
df_high_priority = df_merged[df_merged['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])]
df_low_priority = df_merged[~df_merged['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])]

# Group and count line items by shipping mode for high-priority
df_high_priority_count = df_high_priority.groupby('L_SHIPMODE').size().reset_index(name='HIGH_LINE_COUNT')

# Group and count line items by shipping mode for low-priority
df_low_priority_count = df_low_priority.groupby('L_SHIPMODE').size().reset_index(name='LOW_LINE_COUNT')

# Merge the counts
df_counts = pd.merge(df_high_priority_count, df_low_priority_count, on='L_SHIPMODE', how='outer')

# Sorting the results by shipping mode
df_counts.sort_values('L_SHIPMODE', inplace=True)

# Output to CSV file
df_counts.to_csv('query_output.csv', index=False)
