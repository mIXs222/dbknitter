import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Query partsupp from MySQL
try:
    with mysql_connection.cursor() as cursor:
        cursor.execute("""SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST 
                          FROM partsupp""")
        partsupp_data = cursor.fetchall()
        df_partsupp = pd.DataFrame(partsupp_data, columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_SUPPLYCOST'])
finally:
    mysql_connection.close()

# Connect to Redis and get data
redis_connection = DirectRedis(host='redis', port=6379, db=0)
df_nation = pd.DataFrame(redis_connection.get('nation'), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
df_region = pd.DataFrame(redis_connection.get('region'), columns=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])
df_part = pd.DataFrame(redis_connection.get('part'), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])
df_supplier = pd.DataFrame(redis_connection.get('supplier'), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Filter data as per the query condition
df_europe_nations = df_nation[df_nation['N_REGIONKEY'] == df_region[df_region['R_NAME'] == 'EUROPE']['R_REGIONKEY'].values[0]]
df_brass_parts = df_part[(df_part['P_TYPE'] == 'BRASS') & (df_part['P_SIZE'] == 15)]

# Join and compute per conditions
df_result = (
    df_partsupp
    .merge(df_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    .merge(df_brass_parts, left_on='PS_PARTKEY', right_on='P_PARTKEY')
    .merge(df_europe_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .query('P_TYPE == "BRASS" & P_SIZE == 15')
)

# Select suppliers with minimum cost for each part in EUROPE region
df_result['min_cost'] = df_result.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].transform('min')
df_final = df_result[df_result['PS_SUPPLYCOST'] == df_result['min_cost']].copy()

# Sorting based on the given criteria
df_final.sort_values(
    by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'PS_PARTKEY'],
    ascending=[False, True, True, True],
    inplace=True
)

# Selecting required columns
df_final = df_final[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'PS_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]

# Write final result to CSV file
df_final.to_csv('query_output.csv', index=False)
