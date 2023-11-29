# import required modules
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Execute the query for MySQL to fetch part data with BRAND#23 and MED BAG
mysql_query = """
SELECT P_PARTKEY
FROM part
WHERE P_BRAND = 'BRAND#23' AND P_CONTAINER = 'MED BAG'
"""
mysql_cursor.execute(mysql_query)
part_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY'])

# Close MySQL cursor and connection
mysql_cursor.close()
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch lineitem table from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'))
redis_conn.close()

# Filter lineitem for parts in part_df and compute the average lineitem quantity
filtered_lineitem_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(part_df['P_PARTKEY'])]

# Calculate the average quantity
average_quantity = filtered_lineitem_df['L_QUANTITY'].mean()

# Find lineitems with quantity less than 20% of average
low_quantity_lineitems = filtered_lineitem_df[filtered_lineitem_df['L_QUANTITY'] < (0.20 * average_quantity)]

# Calculate yearly undiscouted gross loss in revenue
low_quantity_lineitems['LOSS'] = low_quantity_lineitems['L_QUANTITY'] * low_quantity_lineitems['L_EXTENDEDPRICE']
average_yearly_loss = low_quantity_lineitems['LOSS'].sum() / 7  # assuming 'lineitem' covers data for 7 years

# Save the result to CSV
result_df = pd.DataFrame({'Average_Yearly_Loss': [average_yearly_loss]})
result_df.to_csv('query_output.csv', index=False)
