# multi_database_query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query parts from MySQL
mysql_query = """
SELECT P_PARTKEY, P_BRAND, P_SIZE, P_CONTAINER
FROM part
WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
   OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
   OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""
parts_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query lineitems from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Merging and filtering DataFrames
result_df = lineitem_df.merge(parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Apply additional filters and calculate the revenues
result_df = result_df[
    ((result_df['P_BRAND'] == 'Brand#12') & result_df['L_QUANTITY'].between(1, 11)) |
    ((result_df['P_BRAND'] == 'Brand#23') & result_df['L_QUANTITY'].between(10, 20)) |
    ((result_df['P_BRAND'] == 'Brand#34') & result_df['L_QUANTITY'].between(20, 30))
]
result_df = result_df[result_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])]
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Calculate the sum of the revenue
final_result = result_df[['REVENUE']].sum()

# Output to csv file
final_result.to_csv('query_output.csv', index=False)

