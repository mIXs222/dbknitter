import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql', 
                                   user='root', 
                                   password='my-secret-pw', 
                                   db='tpch', 
                                   charset='utf8mb4')

# Query to find European region key
europe_query = "SELECT R_REGIONKEY FROM region WHERE R_NAME = 'EUROPE'"
europe_region_df = pd.read_sql(europe_query, mysql_connection)
europe_region_key = europe_region_df.iloc[0]['R_REGIONKEY']

# Query for parts and nations
parts_query = """
SELECT P_PARTKEY, P_MFGR FROM part
WHERE P_TYPE = 'BRASS' AND P_SIZE = 15
"""
nations_query = """
SELECT N_NATIONKEY, N_NAME FROM nation
WHERE N_REGIONKEY = %s
"""
parts_df = pd.read_sql(parts_query, mysql_connection)
nations_df = pd.read_sql(nations_query, mysql_connection, params=(europe_region_key,))

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Fetch partsupp table from Redis
partsupp_df = pd.DataFrame(redis_connection.get('partsupp'))

# Join tables and filter for minimum cost suppliers
result_df = partsupp_df.merge(parts_df, left_on='PS_PARTKEY', right_on='P_PARTKEY').merge(nations_df, left_on='PS_SUPPKEY', right_on='N_NATIONKEY')

# Query to get supplier details for relevant suppliers
supplier_query = """
SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, S_ACCTBAL, S_COMMENT, S_NATIONKEY
FROM supplier
"""
supplier_df = pd.read_sql(supplier_query, mysql_connection)
result_df = result_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Find minimum cost suppliers
result_df['min_cost'] = result_df.groupby('P_PARTKEY')['PS_SUPPLYCOST'].transform('min')
result_df = result_df[result_df['PS_SUPPLYCOST'] == result_df['min_cost']]

# Sort the results as per the requirement
sorted_df = result_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Select the required columns
final_df = sorted_df[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]

# Write to CSV
final_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_connection.close()
