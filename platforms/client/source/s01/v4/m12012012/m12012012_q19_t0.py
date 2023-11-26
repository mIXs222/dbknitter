# Python Code: query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Query part from MySQL
part_query = """
SELECT P_PARTKEY, P_BRAND, P_CONTAINER, P_SIZE
FROM part
WHERE P_BRAND IN ('Brand#12', 'Brand#23', 'Brand#34')
  AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
  AND P_SIZE BETWEEN 1 AND 15;
"""

part_df = pd.read_sql(part_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Read lineitem from Redis
lineitem_df = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_df)

# Merge DataFrames
merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Additional filtering based on the query logic
conditions = (
    (
        (merged_df['P_BRAND'] == 'Brand#12') &
        (merged_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
        (merged_df['L_QUANTITY'] >= 1) &
        (merged_df['L_QUANTITY'] <= 11) &
        (merged_df['P_SIZE'] >= 1) &
        (merged_df['P_SIZE'] <= 5) &
        (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
        (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    ) |
    (
        (merged_df['P_BRAND'] == 'Brand#23') &
        (merged_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
        (merged_df['L_QUANTITY'] >= 10) &
        (merged_df['L_QUANTITY'] <= 20) &
        (merged_df['P_SIZE'] >= 1) &
        (merged_df['P_SIZE'] <= 10) &
        (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
        (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    ) |
    (
        (merged_df['P_BRAND'] == 'Brand#34') &
        (merged_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
        (merged_df['L_QUANTITY'] >= 20) &
        (merged_df['L_QUANTITY'] <= 30) &
        (merged_df['P_SIZE'] >= 1) &
        (merged_df['P_SIZE'] <= 15) &
        (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
        (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    )
)

# Apply conditions to the merged data
final_df = merged_df[conditions]

# Calculating revenue
final_df['REVENUE'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])

# Aggregating revenue
result_df = final_df.groupby('P_PARTKEY').agg({'REVENUE': 'sum'}).reset_index()

# Write output to CSV
result_df.to_csv('query_output.csv', index=False)
