import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to mysql database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Get data from MySQL
mysql_cursor.execute("SELECT * FROM region WHERE R_NAME = 'EUROPE'")
regions_europe = {row[0]: row for row in mysql_cursor.fetchall()}
europe_region_keys = regions_europe.keys()

mysql_cursor.execute("SELECT * FROM nation WHERE N_REGIONKEY IN (%s)" % ','.join(['%s'] * len(europe_region_keys)), tuple(europe_region_keys))
nations = {row[0]: row for row in mysql_cursor.fetchall()}

mysql_cursor.execute("SELECT P_PARTKEY, P_NAME, P_MFGR FROM part WHERE P_TYPE = 'BRASS' AND P_SIZE = 15")
parts = {row[0]: list(row[1:]) for row in mysql_cursor.fetchall()}

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis into pandas DataFrames
supplier_df = pd.DataFrame(redis_conn.get('supplier'))
partsupp_df = pd.DataFrame(redis_conn.get('partsupp'))

# Filter data as per the query requirements
valid_nation_keys = [key for key, value in nations.items() if value[2] in europe_region_keys]
filtered_supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(valid_nation_keys) & supplier_df['S_NATIONKEY'].notnull()]

filtered_partsupp_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(parts.keys()) & partsupp_df['PS_AVAILQTY'] > 0]

# Merge dataframes and select minimum cost suppliers
merged_df = pd.merge(filtered_supplier_df, filtered_partsupp_df, how='inner', left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
merged_df['P_MFGR'] = merged_df['PS_PARTKEY'].apply(lambda x: parts[x][1])

result_df = merged_df.groupby('PS_PARTKEY').apply(lambda x: x.loc[x['PS_SUPPLYCOST'].idxmin() if x['PS_SUPPLYCOST'].idxmin() is not pd.NaT else x.index[0]])

# Sort the result as required in the query
result_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Select and rename columns as per the query requirements
output_df = result_df[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']].copy()
output_df.columns = ['SUPPLIER_ACCTBAL', 'SUPPLIER_NAME', 'NATION_NAME', 'PART_KEY', 'MANUFACTURER', 'ADDRESS', 'PHONE', 'COMMENT']

# Write results to CSV
output_df.to_csv('query_output.csv', index=False)
