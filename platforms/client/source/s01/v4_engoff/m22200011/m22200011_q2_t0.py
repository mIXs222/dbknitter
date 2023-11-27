# query.py
import pymysql
import pandas as pd
import direct_redis

# MySQL connection
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Query to get supplier and partsupp data from MySQL
supplier_query = """
SELECT S_SUPPKEY, S_ACCTBAL, S_NAME, S_ADDRESS, S_PHONE, S_COMMENT, N_NAME
FROM supplier
INNER JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
WHERE N_REGIONKEY = (
    SELECT R_REGIONKEY
    FROM region
    WHERE R_NAME = 'EUROPE'
)
"""

partsupp_query = """
SELECT PS_SUPPKEY, PS_PARTKEY, PS_SUPPLYCOST
FROM partsupp
"""

# Execute the queries
supplier_df = pd.read_sql(supplier_query, mysql_connection)
partsupp_df = pd.read_sql(partsupp_query, mysql_connection)

# Redis connection
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query Redis for data
nation_df = pd.DataFrame(redis_connection.get('nation'))
region_df = pd.DataFrame(redis_connection.get('region'))
part_df = pd.DataFrame(redis_connection.get('part'))
part_df = part_df[(part_df['P_TYPE'] == 'BRASS') & (part_df['P_SIZE'] == 15)]

# Find Europe region key
europe_key = region_df[region_df['R_NAME'] == 'EUROPE']['R_REGIONKEY'].iloc[0]

# Filter nation by Europe region
nation_df = nation_df[nation_df['N_REGIONKEY'] == europe_key]

# Merge dataframes
merged_df = supplier_df.merge(part_df, left_on='S_SUPPKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(partsupp_df, on=['PS_SUPPKEY', 'PS_PARTKEY'])
merged_df = merged_df.merge(nation_df, on='N_NATIONKEY')

# Filter parts supplied by Europe's suppliers at the minimum cost for size 15 BRASS parts
merged_df = merged_df.groupby('PS_PARTKEY').apply(lambda x: x[x['PS_SUPPLYCOST'] == x['PS_SUPPLYCOST'].min()])

# Sort according to the criteria
merged_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'PS_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Select the specified columns
final_columns = [
    'S_ACCTBAL', 'S_NAME', 'N_NAME', 'PS_PARTKEY', 'P_MFGR', 
    'S_ADDRESS', 'S_PHONE', 'S_COMMENT'
]
final_output = merged_df[final_columns]

# Write the output to a CSV file
final_output.to_csv('query_output.csv', index=False)

# Close the MySQL connection
mysql_connection.close()
