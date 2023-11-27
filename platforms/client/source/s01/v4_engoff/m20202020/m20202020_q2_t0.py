import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = connection.cursor()

# Execute query to get region information for EUROPE region key
cursor.execute("SELECT R_REGIONKEY FROM region WHERE R_NAME = 'EUROPE'")
region_key = cursor.fetchone()[0]

# Get nation keys for the given region key
cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_REGIONKEY = %s", (region_key,))
nation_keys = cursor.fetchall()

# Turn tuple of tuples into list
nation_keys_list = [item[0] for item in nation_keys]

# Close MySQL connection
cursor.close()
connection.close()

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get dataframes from Redis
df_nation = pd.read_json(r.get('nation') or "[]")
df_part = pd.read_json(r.get('part') or "[]")
df_partsupp = pd.read_json(r.get('partsupp') or "[]")

# Filter the DataFrames for the next operations
df_part = df_part[(df_part.P_TYPE == 'BRASS') & (df_part.P_SIZE == 15)]
df_nation = df_nation[df_nation['N_NATIONKEY'].isin(nation_keys_list)]

# Perform the join operation using pandas
df_partsupp_min_cost = df_partsupp.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].min().reset_index()
result = pd.merge(df_partsupp, df_partsupp_min_cost, on=['PS_PARTKEY', 'PS_SUPPLYCOST'])
result = pd.merge(result, df_part, left_on='PS_PARTKEY', right_on='P_PARTKEY')
result = pd.merge(result, df_nation, left_on='PS_SUPPKEY', right_on='N_NATIONKEY')

# Sort the result according to the query requirements
result = result.sort_values(
    by=['PS_SUPPLYCOST', 'S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
    ascending=[True, False, True, True, True]
)

# Select and rename appropriate columns
result = result[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]
result.columns = ['Supplier Account Balance', 'Supplier Name', 'Nation Name', 'Part Number', 'Manufacturer', 'Supplier Address', 'Phone Number', 'Comment']

# Write the result to CSV
result.to_csv('query_output.csv', index=False)
