# query.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection parameters
mysql_conn_params = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch",
}

# Connect to MySQL and get parts that match the criteria
conn = pymysql.connect(**mysql_conn_params)
query_mysql = """
SELECT P_PARTKEY, P_TYPE, P_SIZE
FROM part
WHERE P_BRAND <> 'Brand#45' AND P_TYPE NOT LIKE 'MEDIUM POLISHED%' AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
"""
parts = pd.read_sql(query_mysql, conn)
conn.close()

# Redis connection parameters
redis_conn_params = {
    "host": "redis",
    "port": 6379,
    "db": 0,
}

# Connect to Redis and get suppliers and partsupp
redis_db = DirectRedis(**redis_conn_params)
supplier_df = pd.read_json(redis_db.get('supplier'))
partsupp_df = pd.read_json(redis_db.get('partsupp'))

# Filter suppliers with no complaints and merge with partsupp
filtered_suppliers = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer Complaints')]
supp_partsupp = pd.merge(filtered_suppliers, partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Merge with the parts from MySQL to get the final result
final_df = pd.merge(supp_partsupp, parts, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Group by P_TYPE, P_SIZE and count distinct suppliers
result = final_df.groupby(['P_TYPE', 'P_SIZE'])['S_SUPPKEY'].nunique().reset_index()
result.rename(columns={'S_SUPPKEY': 'SupplierCount'}, inplace=True)

# Sort the results as required and save to CSV
result.sort_values(['SupplierCount', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True], inplace=True)
result.to_csv('query_output.csv', index=False)
