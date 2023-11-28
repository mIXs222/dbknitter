# File: query_code.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Execute query to get relevant parts from MySQL
part_query = """
SELECT * FROM part
WHERE ((P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))
    OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))
    OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')))
AND P_SIZE BETWEEN 1 AND 15
"""
parts_df = pd.read_sql(part_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Perform the comprehensive analysis
lineitem_df = lineitem_df.merge(parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Define the conditions
conditions = [
    (lineitem_df['P_BRAND'] == 'Brand#12') & (lineitem_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (lineitem_df['L_QUANTITY'] >= 1) & (lineitem_df['L_QUANTITY'] <= 11) & (lineitem_df['P_SIZE'] >= 1) & (lineitem_df['P_SIZE'] <= 5) & (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),
    (lineitem_df['P_BRAND'] == 'Brand#23') & (lineitem_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & (lineitem_df['L_QUANTITY'] >= 10) & (lineitem_df['L_QUANTITY'] <= 20) & (lineitem_df['P_SIZE'] >= 1) & (lineitem_df['P_SIZE'] <= 10) & (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),
    (lineitem_df['P_BRAND'] == 'Brand#34') & (lineitem_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (lineitem_df['L_QUANTITY'] >= 20) & (lineitem_df['L_QUANTITY'] <= 30) & (lineitem_df['P_SIZE'] >= 1) & (lineitem_df['P_SIZE'] <= 15) & (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
]

# Apply the conditions to filter the DataFrame
selected_lineitems = lineitem_df[conditions[0] | conditions[1] | conditions[2]]

# Calculate revenue
selected_lineitems['REVENUE'] = selected_lineitems['L_EXTENDEDPRICE'] * (1 - selected_lineitems['L_DISCOUNT'])

# Group by the criteria and sum the revenue
result = selected_lineitems.groupby(['P_BRAND', 'P_CONTAINER'])['REVENUE'].sum().reset_index()

# Write output to CSV
result.to_csv('query_output.csv', index=False)
