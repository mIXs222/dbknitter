# python_code.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Execute query on MySQL - for tables: partsupp
partsupp_query = """
SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST
FROM partsupp"""
partsupp_df = pd.read_sql(partsupp_query, mysql_conn)

# Redis - get dataframes
region_df = pd.json_normalize(redis_conn.get("region").to_dict())
nation_df = pd.json_normalize(redis_conn.get("nation").to_dict())
part_df = pd.json_normalize(redis_conn.get("part").to_dict())
supplier_df = pd.json_normalize(redis_conn.get("supplier").to_dict())

# Filter the Redis dataframes as per the criteria mentioned
part_df_filtered = part_df[(part_df.P_SIZE == 15) & (part_df.P_TYPE.str.contains('BRASS'))]
europe_nations = region_df[region_df.R_NAME == 'EUROPE']['R_REGIONKEY']
nation_df_filtered = nation_df[nation_df.N_REGIONKEY.isin(europe_nations)]
supplier_df_filtered = supplier_df[supplier_df.S_NATIONKEY.isin(nation_df_filtered.N_NATIONKEY)]

# Merge DataFrames to match the SQL join operation
merged_df = partsupp_df.merge(part_df_filtered, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(supplier_df_filtered, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Selecting only rows with the minimum PS_SUPPLYCOST for each PS_PARTKEY, PS_SUPPKEY pair within the EUROPE region
merged_df = merged_df.loc[merged_df.groupby(['PS_PARTKEY', 'PS_SUPPKEY'])['PS_SUPPLYCOST'].idxmin()]

# Sorting as per the instructions
result_df = merged_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Selecting required columns
output_df = result_df[['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE']]

# Write to CSV file
output_df.to_csv('query_output.csv', index=False)

# Close the database connections
mysql_conn.close()
redis_conn.close()
