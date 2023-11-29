import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute query for the 'region' and 'supplier' tables in MySQL
with mysql_conn.cursor() as cursor:
    # Get keys for the EUROPE region
    cursor.execute('SELECT R_REGIONKEY FROM region WHERE R_NAME = "EUROPE"')
    europe_keys = [row[0] for row in cursor.fetchall()]
    
    # Find suppliers in the EUROPE region
    cursor.execute('''
        SELECT S_SUPPKEY, S_ACCTBAL, S_NAME, S_ADDRESS, S_PHONE, S_COMMENT, S_NATIONKEY
        FROM supplier
        WHERE S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_REGIONKEY IN %s)
    ''', (europe_keys,))
    suppliers = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'S_NATIONKEY'])

# Close MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch tables from Redis as Pandas DataFrames
nation_df = pd.DataFrame(redis_conn.get('nation'), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
part_df = pd.DataFrame(redis_conn.get('part'), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])
partsupp_df = pd.DataFrame(redis_conn.get('partsupp'), columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])

# Merge the DataFrames to get the necessary information
merged_df = (
    partsupp_df[partsupp_df['PS_PARTKEY'].isin(part_df[(part_df['P_TYPE'] == 'BRASS') & (part_df['P_SIZE'] == 15)]['P_PARTKEY'])]
    .merge(suppliers, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation_df[nation_df['N_REGIONKEY'].isin(europe_keys)], how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(part_df[part_df['P_TYPE'] == 'BRASS'], how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')
)

# Group and sort the data
sorted_df = (
    merged_df.groupby(['PS_PARTKEY', 'S_SUPPKEY'])
    .apply(lambda x: x.nsmallest(1, 'PS_SUPPLYCOST'))
    .sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])
    .reset_index(drop=True)
)

# Select and rename the necessary columns
result_df = sorted_df[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]

# Write output to CSV
result_df.to_csv('query_output.csv', index=False)
