import pymysql
import pandas as pd
import json
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_connection = pymysql.connect(
    user='root',
    password='my-secret-pw',
    host='mysql',
    database='tpch'
)

# Query to get the part data according to the requirements
mysql_query = """
SELECT P_PARTKEY
FROM part
WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
   OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
   OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""

# Execute MySQL query and get data
with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    part_keys = cursor.fetchall()

# Format part_keys for Redis query
part_keys = [pk[0] for pk in part_keys]

# Close MySQL connection
mysql_connection.close()

# Connect to Redis database
redis_client = DirectRedis(host='redis', port=6379, decode_responses=True, db=0)

# Read lineitem table from Redis as Pandas DataFrame
lineitem_df_json = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_df_json)

# Filter lineitem DataFrame according to the requirements
filtered_df = lineitem_df[
    (lineitem_df['L_PARTKEY'].isin(part_keys)) &
    (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON') &
    (
        ((lineitem_df['L_QUANTITY'] >= 1) & (lineitem_df['L_QUANTITY'] <= 11)) |
        ((lineitem_df['L_QUANTITY'] >= 10) & (lineitem_df['L_QUANTITY'] <= 20)) |
        ((lineitem_df['L_QUANTITY'] >= 20) & (lineitem_df['L_QUANTITY'] <= 30))
    )
]

# Calculate revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by and sum revenue
revenue_result = pd.DataFrame(filtered_df.groupby(by=[])['REVENUE'].sum()).reset_index()

# Save result to a CSV file
revenue_result.to_csv('query_output.csv', index=False)
