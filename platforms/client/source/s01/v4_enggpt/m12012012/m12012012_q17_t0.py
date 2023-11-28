import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_query = """
    SELECT P_PARTKEY, P_NAME, P_MFGR, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE
    FROM part
    WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG';
"""
part_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
lineitem_raw = redis_conn.get('lineitem')
lineitem_df = pd.read_json(lineitem_raw)

# Filter the lineitem DataFrame to include only relevant part keys from MySQL data
relevant_part_keys = part_df['P_PARTKEY'].unique()
filtered_lineitem = lineitem_df[lineitem_df['L_PARTKEY'].isin(relevant_part_keys)]

# Calculate the average quantity of each part
avg_quantity_per_part = filtered_lineitem.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_quantity_per_part['20%_avg_quantity'] = avg_quantity_per_part['L_QUANTITY'] * 0.2

# Filter line items where the quantity is less than 20% of the avg quantity of the part
filtered_lineitem = filtered_lineitem.merge(avg_quantity_per_part[['L_PARTKEY', '20%_avg_quantity']], on='L_PARTKEY')
filtered_lineitem = filtered_lineitem[filtered_lineitem['L_QUANTITY'] < filtered_lineitem['20%_avg_quantity']]

# Merge the filtered line items with part details
final_df = filtered_lineitem.merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the average yearly extended price
final_df['avg_yearly_extended_price'] = final_df['L_EXTENDEDPRICE'] / 7.0
final_df = final_df.groupby(['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE'], as_index=False)['avg_yearly_extended_price'].mean()

# Write the final result to a CSV file
final_df.to_csv('query_output.csv', index=False)
