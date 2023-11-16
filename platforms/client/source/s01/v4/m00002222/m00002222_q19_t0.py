import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Execute part table query in MySQL
part_sql_query = """
SELECT * FROM part
WHERE 
  (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5) OR
  (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10) OR
  (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""
part_df = pd.read_sql(part_sql_query, mysql_connection)

# Disconnect MySQL
mysql_connection.close()

# Get lineitem DataFrame from Redis
lineitem = redis_connection.get('lineitem')
lineitem_df = pd.read_json(lineitem)

# Filter lineitem DataFrame based on conditions
filtered_lineitem_df = lineitem_df[
    ((lineitem_df['L_QUANTITY'] >= 1) & (lineitem_df['L_QUANTITY'] <= 1 + 10) & (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |
    ((lineitem_df['L_QUANTITY'] >= 10) & (lineitem_df['L_QUANTITY'] <= 10 + 10) & (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |
    ((lineitem_df['L_QUANTITY'] >= 20) & (lineitem_df['L_QUANTITY'] <= 20 + 10) & (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))
]

# Merge part and lineitem dataframes on P_PARTKEY and L_PARTKEY
merged_df = pd.merge(part_df, filtered_lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Calculate revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Sum up revenue
result_df = merged_df.groupby('P_PARTKEY').agg({'REVENUE': 'sum'}).reset_index()

# Write the result to a csv file
result_df.to_csv('query_output.csv', index=False)
