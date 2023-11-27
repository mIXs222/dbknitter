# query.py
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

# Execute query on MySQL to get relevant 'orders' and 'lineitem' data
mysql_query = """
SELECT s.S_SUPPKEY, s.S_NAME
FROM supplier s
INNER JOIN lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
INNER JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
INNER JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE n.N_NAME = 'SAUDI ARABIA'
AND o.O_ORDERSTATUS = 'F'
AND l.L_COMMITDATE < l.L_RECEIPTDATE
GROUP BY s.S_SUPPKEY, s.S_NAME
HAVING COUNT(DISTINCT l.L_ORDERKEY) > 1
AND SUM(CASE WHEN l.L_COMMITDATE >= l.L_RECEIPTDATE THEN 1 ELSE 0 END) = 0;
"""

with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute(mysql_query)
        mysql_result = cursor.fetchall()

# Convert MySQL results to DataFrame
supplier_df = pd.DataFrame(mysql_result, columns=['S_SUPPKEY', 'S_NAME'])

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'nation' and 'supplier' data from Redis
nation_df = pd.read_json(redis_conn.get('nation'))
supplier_df_redis = pd.read_json(redis_conn.get('supplier'))

# Merge Redis data with MySQL data
merged_df = supplier_df.merge(supplier_df_redis, left_on='S_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter merged data for 'SAUDI ARABIA'
saudi_suppliers_df = merged_df[merged_df['N_NAME'] == 'SAUDI ARABIA']

# Write output to CSV
saudi_suppliers_df.to_csv('query_output.csv', index=False)
