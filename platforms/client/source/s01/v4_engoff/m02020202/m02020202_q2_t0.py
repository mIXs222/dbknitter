import os
import csv
import pymysql
import pandas as pd

# Connecting to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

try:
    # Fetch relevant data from MySQL
    with mysql_conn.cursor() as cursor:
        query = """
                SELECT n.N_NAME, n.N_NATIONKEY, p.P_PARTKEY, p.P_MFGR,
                       ps.PS_SUPPKEY, ps.PS_SUPPLYCOST
                FROM nation n
                JOIN partsupp ps ON n.N_NATIONKEY = ps.PS_SUPPKEY
                JOIN part p ON ps.PS_PARTKEY = p.P_PARTKEY
                WHERE p.P_TYPE = 'BRASS' AND p.P_SIZE = 15
               """
        cursor.execute(query)
        mysql_data = list(cursor.fetchall())

finally:
    mysql_conn.close()

# Transform MySQL data into DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=['N_NAME', 'N_NATIONKEY',
                                             'P_PARTKEY', 'P_MFGR',
                                             'PS_SUPPKEY', 'PS_SUPPLYCOST'])

# Connecting to the Redis database using DirectRedis for Pandas DataFrame
from direct_redis import DirectRedis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch relevant data from Redis
region_df = pd.read_json(redis_conn.get('region'))
supplier_df = pd.read_json(redis_conn.get('supplier'))

# Join and filter data
europe_region = region_df[region_df['R_NAME'] == 'EUROPE']
result = pd.merge(europe_region, supplier_df, left_on='R_REGIONKEY', right_on='S_NATIONKEY')
result = pd.merge(result, mysql_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Find the minimum supply cost for each part
result = result.assign(min_cost=result.groupby('P_PARTKEY')['PS_SUPPLYCOST'].transform('min'))
result = result[result['PS_SUPPLYCOST'] == result['min_cost']]

# Sort data according to given rules
result_sorted = result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
                                   ascending=[False, True, True, True])

# Select and rename the required columns
output = result_sorted[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR',
                        'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]

# Write to CSV
output.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

print("Query results have been successfully saved to query_output.csv.")
