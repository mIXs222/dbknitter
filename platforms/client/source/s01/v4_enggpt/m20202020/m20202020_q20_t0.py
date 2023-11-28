import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Query to get Canadian suppliers from MySQL
canadian_suppliers_query = """
SELECT S_SUPPKEY, S_NAME, S_ADDRESS
FROM supplier s
JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE n.N_NAME = 'CANADA';
"""
with mysql_connection.cursor() as cursor:
    cursor.execute(canadian_suppliers_query)
    canadian_suppliers_df = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS'])

# Get 'part' and 'partsupp' as DataFrame from Redis
partsupp_df = pd.read_json(redis_connection.get('partsupp'), orient='split')
part_df = pd.read_json(redis_connection.get('part'), orient='split')

# Filter parts named 'forest' and get their keys
forest_parts_df = part_df[part_df['P_NAME'].str.startswith('forest')]
forest_parts_supp_keys = partsupp_df[
    partsupp_df['PS_PARTKEY'].isin(forest_parts_df['P_PARTKEY'])]['PS_SUPPKEY'].unique()

# Get lineitem table from MySQL
lineitem_query = """
SELECT L_PARTKEY, L_SUPPKEY, SUM(L_QUANTITY) AS TOTAL_QUANTITY
FROM lineitem
WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
GROUP BY L_PARTKEY, L_SUPPKEY
HAVING TOTAL_QUANTITY > (SELECT 0.5 * SUM(L_QUANTITY) FROM lineitem
                         WHERE L_PARTKEY = lineitem.L_PARTKEY AND L_SUPPKEY = lineitem.L_SUPPKEY);
"""
with mysql_connection.cursor() as cursor:
    cursor.execute(lineitem_query)
    lineitem_sum_df = pd.DataFrame(cursor.fetchall(), columns=['L_PARTKEY', 'L_SUPPKEY', 'TOTAL_QUANTITY'])

# Get supplier keys from 'lineitem_sum_df' that are in 'forest_parts_supp_keys'
filtered_supplier_keys = lineitem_sum_df[
    lineitem_sum_df['L_SUPPKEY'].isin(forest_parts_supp_keys)]['L_SUPPKEY'].unique()

# Filter Canadian suppliers who meet the line item conditions
canadian_final_suppliers_df = canadian_suppliers_df[
    canadian_suppliers_df['S_SUPPKEY'].isin(filtered_supplier_keys)]

# Order results by S_NAME
canadian_final_suppliers_df.sort_values('S_NAME', inplace=True)

# Write to CSV
canadian_final_suppliers_df.to_csv('query_output.csv', index=False)

# Close database connections
mysql_connection.close()
redis_connection.close()
