import csv
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_connection.cursor()

# Extract orders with URGENT or HIGH priority
mysql_cursor.execute("""
    SELECT O_ORDERKEY, O_ORDERPRIORITY FROM orders 
    WHERE O_ORDERPRIORITY = 'URGENT' OR O_ORDERPRIORITY = 'HIGH'
""")
urgent_high_priority_orders = {row[0]: row[1] for row in mysql_cursor.fetchall()}

mysql_connection.close()

# Connect to Redis database
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Read lineitem dataframe from Redis
lineitem_df = pd.read_msgpack(redis_connection.get('lineitem'))

# Filter for specified conditions
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

filtered_lineitems = lineitem_df[(lineitem_df['L_RECEIPTDATE'] >= '1994-01-01') &\
                                 (lineitem_df['L_RECEIPTDATE'] <= '1995-01-01') &\
                                 (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']) &\
                                 (lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']) &\
                                 (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP']))]

# Split into two groups based on order priority
filtered_lineitems['PRIORITY_GROUP'] = filtered_lineitems['L_ORDERKEY'].map(
    lambda x: 'URGENT/HIGH' if x in urgent_high_priority_orders else 'OTHER'
)

# Count the number of late lineitems by ship mode and priority group
result_df = filtered_lineitems.groupby(['L_SHIPMODE', 'PRIORITY_GROUP']).size().reset_index(name='LATE_LINEITEMS_COUNT')

# Write the output to a CSV file
result_df.to_csv('query_output.csv', index=False)

print('Query executed successfully. Results are saved in query_output.csv.')
