import pandas as pd
import pymysql
from sqlalchemy import create_engine
import direct_redis

# Connect to MySQL
mysql_conn = create_engine('mysql+pymysql://root:my-secret-pw@mysql/tpch')

# Fetch data from MySQL database
nation_query = "SELECT * FROM nation WHERE N_REGIONKEY IN (SELECT R_REGIONKEY FROM region WHERE R_NAME = 'EUROPE')"
region_query = "SELECT * FROM region WHERE R_NAME = 'EUROPE'"
part_query = "SELECT * FROM part WHERE P_SIZE = 15 AND P_TYPE LIKE '%BRASS'"
supplier_query = "SELECT * FROM supplier WHERE S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_REGIONKEY IN (SELECT R_REGIONKEY FROM region WHERE R_NAME = 'EUROPE'))"

nation_df = pd.read_sql(nation_query, mysql_conn)
region_df = pd.read_sql(region_query, mysql_conn)
part_df = pd.read_sql(part_query, mysql_conn)
supplier_df = pd.read_sql(supplier_query, mysql_conn)

# Connect to Redis and fetch partsupp data
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
partsupp_df = pd.read_json(redis_connection.get('partsupp'))

# We have all the dataframes, but partsupp_df needs to be filtered by minimum PS_SUPPLYCOST
# for each part.

# First, find the minimum PS_SUPPLYCOST for each PS_PARTKEY
partsupp_min_cost_df = partsupp_df.loc[partsupp_df.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].idxmin()]

# Then, merge dataframes
merged_df = pd.merge(supplier_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = pd.merge(merged_df, partsupp_min_cost_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
merged_df = pd.merge(merged_df, part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df[merged_df['N_REGIONKEY'] == region_df.iloc[0]['R_REGIONKEY']]

# Filter out columns that are not relevant
output_columns = [
    'S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT',
    'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'N_NAME'
]
output_df = merged_df[output_columns]

# Sort the final output
output_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Write data to CSV
output_df.to_csv('query_output.csv', index=False)
