import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL tpch database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# MySQL Query
mysql_query = """
SELECT s.S_ACCTBAL, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_COMMENT, ps.PS_PARTKEY, p.P_MFGR, p.P_SIZE 
FROM supplier AS s 
JOIN nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY 
JOIN region AS r ON n.N_REGIONKEY = r.R_REGIONKEY 
JOIN partsupp AS ps ON s.S_SUPPKEY = ps.PS_SUPPKEY 
JOIN part AS p ON ps.PS_PARTKEY = p.P_PARTKEY 
WHERE r.R_NAME = 'EUROPE' AND p.P_SIZE = 15 AND p.P_TYPE LIKE '%BRASS%' 
ORDER BY s.S_ACCTBAL DESC, n.N_NAME, s.S_NAME, ps.PS_PARTKEY;
"""

# Execute MySQL Query
mysql_cursor.execute(mysql_query)
mysql_result = mysql_cursor.fetchall()

# Convert MySQL result to DataFrame
mysql_columns = ['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE']
mysql_df = pd.DataFrame(mysql_result, columns=mysql_columns)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve and decode data from Redis
nation_df = pd.read_msgpack(redis_client.get('nation'))
part_df = pd.read_msgpack(redis_client.get('part'))
partsupp_df = pd.read_msgpack(redis_client.get('partsupp'))

mysql_conn.close()

# Perform the required merges and filtering for Redis data
redis_result = pd.merge(nation_df[nation_df['N_REGIONKEY'] == 'EUROPE'], partsupp_df, left_on='N_NATIONKEY', right_on='PS_SUPPKEY')
redis_result = pd.merge(redis_result, part_df[(part_df['P_SIZE'] == 15) & (part_df['P_TYPE'].str.contains('BRASS'))], left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Combine MySQL and Redis results
combined_df = pd.merge(mysql_df, redis_result, on=['P_PARTKEY', 'S_NAME'])

# Sort the combined DataFrame as per query requirement
combined_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Write the query output to a CSV file
combined_df.to_csv('query_output.csv', index=False)
