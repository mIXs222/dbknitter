import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Prepare MySQL query
mysql_query = '''
SELECT s.S_SUPPKEY, p.PS_PARTKEY
FROM supplier AS s
JOIN partsupp AS p
ON s.S_SUPPKEY = p.PS_SUPPKEY
WHERE s.S_COMMENT NOT LIKE '%%Customer%%Complaints%%'
'''

# Execute the query and load into a pandas DataFrame
df_supplier_partsupp = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to the Redis server
redis = DirectRedis(host='redis', port=6379, db=0)

# Retrieve the 'part' table data from Redis and load into a pandas DataFrame
part_json = redis.get('part')
df_part = pd.read_json(part_json, orient='records')

# Perform the filtering as per the user's query
size_filter = [49, 14, 23, 45, 19, 3, 36, 9]
filtered_parts = df_part[
    df_part['P_SIZE'].isin(size_filter) &
    df_part['P_TYPE'] != 'MEDIUM POLISHED' &
    (~df_part['P_BRAND'].str.endswith('45'))
]

# Join the dataframes to simulate the full query
result = pd.merge(filtered_parts, df_supplier_partsupp, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Get the final result containing the count of suppliers
final_result = result.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])[['S_SUPPKEY']].nunique()
final_result = final_result.rename(columns={'S_SUPPKEY': 'supplier_count'})
final_result = final_result.reset_index().sort_values(by=['supplier_count', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write the final result to a CSV file
final_result.to_csv('query_output.csv', index=False)
