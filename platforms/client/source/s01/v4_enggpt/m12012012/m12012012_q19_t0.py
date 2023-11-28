import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Define query for the 'part' table in MySQL
part_query = """
SELECT * FROM part 
WHERE 
    (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5) OR
    (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10) OR
    (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""

# Execute query for parts
mysql_cursor.execute(part_query)
part_data = mysql_cursor.fetchall()
part_df = pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve lineitem DataFrame from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Define selection conditions for lineitem
conditions = [
    (lineitem_df['L_PARTKEY'].isin(part_df['P_PARTKEY']) & 
     lineitem_df['L_QUANTITY'].between(1, 11) & 
     lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) & 
     lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),

    (lineitem_df['L_PARTKEY'].isin(part_df['P_PARTKEY']) & 
     lineitem_df['L_QUANTITY'].between(10, 20) & 
     lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) & 
     lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),

    (lineitem_df['L_PARTKEY'].isin(part_df['P_PARTKEY']) & 
     lineitem_df['L_QUANTITY'].between(20, 30) & 
     lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) & 
     lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),
]

# Apply selection criteria and calculate revenue
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
lineitem_df = lineitem_df[conditions[0] | conditions[1] | conditions[2]]

# Sum up the revenue
total_revenue = lineitem_df['REVENUE'].sum()

# Write output to CSV
query_output_df = pd.DataFrame([{'Total Revenue': total_revenue}])
query_output_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_conn.close()
