import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch part data from MySQL
mysql_query = "SELECT * FROM part WHERE P_PROMO = 'PROMO'"
mysql_cursor.execute(mysql_query)
part_data = mysql_cursor.fetchall()

# Transform part data to DataFrame
part_df = pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

# Close the MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch lineitem data from Redis
lineitem_data = redis_conn.get('lineitem')

# Check if 'lineitem' key exists in Redis and if yes, transform data to DataFrame
if lineitem_data:
    lineitem_df = pd.read_json(lineitem_data)
else:
    lineitem_df = pd.DataFrame(columns=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])

# Close the Redis connection
redis_conn.close()

# Filter line items on date
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitems = lineitem_df[(lineitem_df['L_SHIPDATE'] >= pd.Timestamp(1995, 9, 1)) & (lineitem_df['L_SHIPDATE'] <= pd.Timestamp(1995, 10, 1))]

# Calculate revenue
filtered_lineitems['revenue'] = filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])

# Merging to get only promotional parts
promo_parts_revenue = filtered_lineitems[filtered_lineitems['L_PARTKEY'].isin(part_df['P_PARTKEY'])]

# Calculate total and promotional revenue
total_revenue = filtered_lineitems['revenue'].sum()
promo_revenue = promo_parts_revenue['revenue'].sum()

# Calculate promotion effect percentage
promo_effect_percentage = (promo_revenue / total_revenue) * 100 if total_revenue != 0 else 0

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Promotion Effect Percentage'])
    writer.writerow([promo_effect_percentage])
