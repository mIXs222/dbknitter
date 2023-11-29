import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query Redis for parts with BRAND#23 and MED BAG
part_df = pd.read_json(redis_conn.get('part'))
eligible_parts = part_df[(part_df['P_BRAND'] == 'BRAND#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Query MySQL for line items
mysql_cursor.execute("SELECT * FROM lineitem")
lineitem_columns = [desc[0] for desc in mysql_cursor.description]
lineitem_df = pd.DataFrame(mysql_cursor.fetchall(), columns=lineitem_columns)

# Merge eligible parts with line items
merged_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(eligible_parts['P_PARTKEY'])]

# Calculate the average quantity and threshold quantity
avg_quantity = merged_df['L_QUANTITY'].mean()
quantity_threshold = avg_quantity * 0.20

# Filter out the line items with less quantity
low_quantity_df = merged_df[merged_df['L_QUANTITY'] < quantity_threshold]

# Calculate the average yearly gross loss
low_quantity_df['GROSS_LOSS'] = low_quantity_df['L_EXTENDEDPRICE'] * (1 - low_quantity_df['L_DISCOUNT'])
avg_yearly_loss = low_quantity_df['GROSS_LOSS'].sum() / 7

# Write output to a CSV file
output = pd.DataFrame({'Average_Yearly_Loss': [avg_yearly_loss]})
output.to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_conn.close()
