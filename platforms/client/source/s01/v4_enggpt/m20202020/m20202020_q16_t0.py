import pymysql
import pandas as pd
from sqlalchemy import create_engine

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)

# Query for supplier table to exclude suppliers with specific comments
supplier_query = """
SELECT DISTINCT S_SUPPKEY
FROM supplier
WHERE LOWER(S_COMMENT) NOT LIKE '%customer complaints%'
"""
suppliers_df = pd.read_sql(supplier_query, mysql_conn)
suppliers_df.rename(columns={'S_SUPPKEY': 'PS_SUPPKEY'}, inplace=True)

# Close MySQL connection
mysql_conn.close()

# DirectRedis is a placeholder for the actual connection method you need to use for Redis
# Since DirectRedis is not a standard library, this is just an example of how you would use it
# Replace `direct_redis.DirectRedis` with the appropriate connection method to connect to the Redis instance.
import direct_redis

# Connect to Redis and retrieve parts and partsupp tables
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Reading the DataFrame directly from Redis (as per instructions provided)
part_df = redis_conn.get('part')
partsupp_df = redis_conn.get('partsupp')

# Filter the dataframes as per the specified criteria
filtered_part_df = part_df[
    ~(part_df['P_BRAND'].eq('Brand#45')) & 
    ~part_df['P_TYPE'].str.startswith('MEDIUM POLISHED') &
    part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])
]

# Merge tables and perform the final analysis
merged_df = pd.merge(partsupp_df, filtered_part_df, on='P_PARTKEY')
final_df = pd.merge(merged_df, suppliers_df, on='PS_SUPPKEY')

# Group by the required attributes and count distinct suppliers
result_df = final_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['PS_SUPPKEY'].nunique().reset_index(name='SUPPLIER_CNT')

# Order the results as specified in the query
result_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Write the final dataframe to CSV
result_df.to_csv('query_output.csv', index=False)
