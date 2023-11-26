import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Fetching part data from MySQL
mysql_cursor.execute("""
SELECT P_PARTKEY, P_BRAND, P_CONTAINER
FROM part
WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'
""")
part_data = mysql_cursor.fetchall()

# Put part data into a dataframe
part_df = pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_BRAND', 'P_CONTAINER'])

# Connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetching lineitem data from Redis
lineitem_data = redis_conn.get('lineitem')
lineitem_df = pd.read_pickle(lineitem_data)

# Merging and aggregating the data according to the SQL query

# Get the average quantity for each part
avg_qty_per_part = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_qty_per_part['avg_0.2'] = avg_qty_per_part['L_QUANTITY'] * 0.2

# Merge to filter lineitems
filtered_lineitem = pd.merge(
    lineitem_df,
    avg_qty_per_part,
    on='L_PARTKEY',
    how='inner'
)
filtered_lineitem = filtered_lineitem[
    filtered_lineitem['L_QUANTITY'] < filtered_lineitem['avg_0.2']
][['L_PARTKEY', 'L_EXTENDEDPRICE']]

# Merge with part data on partkey and filter according to conditions
result = pd.merge(
    filtered_lineitem,
    part_df,
    left_on='L_PARTKEY',
    right_on='P_PARTKEY',
    how='inner'
)

# Aggregate the result
avg_yearly = result['L_EXTENDEDPRICE'].sum() / 7.0

# Write the output to CSV
output_df = pd.DataFrame({'AVG_YEARLY': [avg_yearly]})
output_df.to_csv('query_output.csv', index=False)

# Clean up
mysql_cursor.close()
mysql_conn.close()
