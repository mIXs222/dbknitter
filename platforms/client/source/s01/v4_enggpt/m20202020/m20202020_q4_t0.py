import pandas as pd
import pymysql
from direct_redis import DirectRedis
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch order data from Redis
orders_df = pd.read_json(redis_conn.get('orders'))

# Prepare and execute MySQL query for line items within the specified date
lineitem_query = """
SELECT L_ORDERKEY
FROM lineitem
WHERE L_COMMITDATE < L_RECEIPTDATE AND L_SHIPDATE BETWEEN '1993-07-01' AND '1993-10-01'
"""
mysql_cursor.execute(lineitem_query)
lineitem_data = mysql_cursor.fetchall()

# Convert line items result to DataFrame
lineitem_df = pd.DataFrame(lineitem_data, columns=['L_ORDERKEY'])

# Merge data based on order key and filter those present in lineitem_df
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate count of orders for each priority
order_priority_counts = merged_df.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].nunique()

# Convert to a DataFrame
output_df = order_priority_counts.reset_index().rename(columns={'O_ORDERKEY': 'count'})

# Sort the DataFrame by order priority
output_df = output_df.sort_values('O_ORDERPRIORITY')

# Write the results to a CSV file
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)

# Close the connections
mysql_conn.close()
redis_conn.close()
