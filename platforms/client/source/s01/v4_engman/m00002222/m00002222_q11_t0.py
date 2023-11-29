# query.py
import pymysql
import pandas as pd
import direct_redis

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Read nation and supplier tables
with mysql_conn.cursor() as cursor:
    # Read nation table
    cursor.execute("SELECT * FROM nation WHERE N_NAME = 'GERMANY'")
    nations_germany = cursor.fetchall()
    df_nations_germany = pd.DataFrame(nations_germany, columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
    
    # Find German nation keys
    german_nation_keys = df_nations_germany['N_NATIONKEY'].tolist()
    
    # Read supplier table and filter for German suppliers only
    cursor.execute(f"SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT FROM supplier WHERE S_NATIONKEY IN ({','.join(map(str, german_nation_keys))})")
    suppliers_germany = cursor.fetchall()
    df_suppliers_germany = pd.DataFrame(suppliers_germany, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Close MySQL connection
mysql_conn.close()

# Connect to the Redis database using direct_redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read partsupp table stored as a pandas dataframe in redis
df_partsupp = pd.read_json(redis_conn.get('partsupp'))

# Calculate total value of all available parts
df_partsupp['PS_TOTAL_VALUE'] = df_german_suppliers['PS_AVAILQTY'] * df_partsupp['PS_SUPPLYCOST']
total_value = df_partsupp['PS_TOTAL_VALUE'].sum()

# Identify all parts that represent a significant percentage of the total value
significant_value = total_value * 0.0001
df_significant_parts = df_partsupp[df_partsupp['PS_TOTAL_VALUE'] > significant_value]

# Final result
result = df_significant_parts[['PS_PARTKEY', 'PS_TOTAL_VALUE']]
result = result.sort_values(by='PS_TOTAL_VALUE', ascending=False)

# Write results to 'query_output.csv'
result.to_csv('query_output.csv', index=False)
