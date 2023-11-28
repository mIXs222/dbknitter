# File: query.py
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import direct_redis

#### MySQL Connection ####
mysql_conn_info = {
    "user": "root",
    "password": "my-secret-pw",
    "host": "mysql",
    "database": "tpch"
}

# Establish a connection to the MySQL database
mysql_conn = pymysql.connect(**mysql_conn_info)

# SQL Query to get partsupp data from MySQL
partsupp_sql = """
SELECT PS_PARTKEY, PS_SUPPKEY
FROM partsupp
"""

partsupp_df = pd.read_sql(partsupp_sql, mysql_conn)
mysql_conn.close()

#### Redis Connection ####
redis_host = 'redis'
redis_port = 6379

# Establish a connection to the Redis data store
redis_conn = direct_redis.DirectRedis(host=redis_host, port=redis_port)

# Get part and supplier data from Redis
part_df = pd.DataFrame(redis_conn.get('part'))
supplier_df = pd.DataFrame(redis_conn.get('supplier'))

# Pre-processing Redis DataFrames
# Convert columns to the correct data types
part_df = part_df.astype({'P_PARTKEY': int})
supplier_df = supplier_df.astype({'S_SUPPKEY': int})

# Filters for part dataframe
exclude_brands = ['Brand#45']
exclude_types = ['MEDIUM POLISHED']
include_sizes = [49, 14, 23, 45, 19, 3, 36, 9]

part_df = part_df[
    (~part_df['P_BRAND'].isin(exclude_brands)) &
    (~part_df['P_TYPE'].str.startswith(tuple(exclude_types))) &
    (part_df['P_SIZE'].isin(include_sizes))
]

# Filter for supplier dataframe based on S_COMMENT
supplier_df = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer Complaints')]

# Merge parts_df with partsupp_df
merged_df = pd.merge(part_df, partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Merge the result with supplier_df
merged_df = pd.merge(merged_df, supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group by 'P_BRAND', 'P_TYPE', 'P_SIZE'
grouped = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])

# Calculate SUPPLIER_CNT for each group
result_df = grouped['S_SUPPKEY'].nunique().reset_index(name='SUPPLIER_CNT')

# Sort the result based on SUPPLIER_CNT (descending), P_BRAND, P_TYPE, and P_SIZE (ascending)
result_df = result_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Save result to CSV
result_df.to_csv('query_output.csv', index=False)
