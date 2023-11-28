# import_libraries.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Setup connection info
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)
query_supplier = "SELECT * FROM supplier WHERE S_NATIONKEY=(SELECT N_NATIONKEY FROM nation WHERE N_NAME='GERMANY')"
suppliers_germany = pd.read_sql(query_supplier, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
nation = pd.DataFrame(redis_conn.get('nation'))
partsupp = pd.DataFrame(redis_conn.get('partsupp'))

# Join the dataframes
nation.columns = ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT']
partsupp.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT']
nation_germany = nation[nation['N_NAME'] == 'GERMANY']
suppliers_germany['S_NATIONKEY'] = suppliers_germany['S_NATIONKEY'].astype(int)
df_joined = pd.merge(partsupp, suppliers_germany, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculate total value and filter as stated
df_joined['TOTAL_VALUE'] = df_joined['PS_SUPPLYCOST'] * df_joined['PS_AVAILQTY']
total_value_threshold = df_joined['TOTAL_VALUE'].sum() * 0.02  # Let's assume 2% threshold
df_result = df_joined.groupby('PS_PARTKEY').filter(lambda x: x['TOTAL_VALUE'].sum() > total_value_threshold)
df_result = df_result.groupby('PS_PARTKEY', as_index=False)['TOTAL_VALUE'].sum()
df_result.sort_values('TOTAL_VALUE', ascending=False, inplace=True)

# Write to CSV
df_result.to_csv('query_output.csv', index=False)
