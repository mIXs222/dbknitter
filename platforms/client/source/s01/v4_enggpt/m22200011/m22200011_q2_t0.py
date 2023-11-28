import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to the MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Query to select details from the MySQL database
mysql_query = """
SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_ACCTBAL, s.S_COMMENT,
p.P_PARTKEY, p.P_MFGR, p.P_SIZE
FROM supplier s JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
WHERE r.R_NAME = 'EUROPE'
AND p.P_SIZE = 15 AND p.P_TYPE LIKE '%BRASS%'
AND ps.PS_SUPPLYCOST = (SELECT MIN(PS_SUPPLYCOST) FROM partsupp WHERE PS_PARTKEY = p.P_PARTKEY)
"""

# Execute the MySQL query
mysql_cursor.execute(mysql_query)
mysql_results = mysql_cursor.fetchall()

# Convert MySQL results to DataFrame
mysql_df = pd.DataFrame(mysql_results, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE'])

# Close the MySQL cursor and connection
mysql_cursor.close()
mysql_conn.close()

# Connect to the Redis server
redis = DirectRedis(host='redis', port=6379, db=0)

# Get nation and part data from Redis and convert to DataFrame
redis_nation_df = pd.read_json(redis.get('nation'))
redis_part_df = pd.read_json(redis.get('part'))

# Merge data from Redis and MySQL
merged_df = pd.merge(mysql_df, redis_part_df, left_on='P_PARTKEY', right_on='P_PARTKEY', how='inner')
merged_df = pd.merge(merged_df, redis_nation_df, left_on='S_SUPPKEY', right_on='N_NATIONKEY', how='inner')

# Filter data based on criteria and sort as required
filtered_df = merged_df[(merged_df['P_SIZE'] == 15) & (merged_df['P_TYPE'].str.contains('BRASS')) & (merged_df['R_NAME'] == 'EUROPE')]
filtered_sorted_df = filtered_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Save the result to CSV
filtered_sorted_df.to_csv('query_output.csv', index=False)
