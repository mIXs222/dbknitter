# suppliers_who_kept_orders_waiting.py
import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

# Query orders and lineitem from MySQL
query = """
SELECT l1.L_SUPPKEY, COUNT(*) AS NUMWAIT
FROM lineitem l1
JOIN orders o ON l1.L_ORDERKEY = o.O_ORDERKEY
WHERE o.O_ORDERSTATUS = 'F'
AND l1.L_COMMITDATE < l1.L_RECEIPTDATE
AND EXISTS (
    SELECT 1
    FROM lineitem l2
    WHERE l1.L_ORDERKEY = l2.L_ORDERKEY
    AND l1.L_SUPPKEY != l2.L_SUPPKEY
)
GROUP BY l1.L_SUPPKEY
HAVING COUNT(*) > 0
ORDER BY NUMWAIT DESC, l1.L_SUPPKEY ASC;
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(query)
    suppliers_result = cursor.fetchall()

# Convert result to DataFrame
suppliers_df = pd.DataFrame(suppliers_result, columns=['S_SUPPKEY', 'NUMWAIT'])

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch nation and supplier data from Redis and convert to DataFrame
nation = pd.read_json(redis_client.get('nation'), orient='records')
supplier = pd.read_json(redis_client.get('supplier'), orient='records')

# Close connections
mysql_conn.close()

# Filter for nation SAUDI ARABIA
saudi_nation_key = nation[nation['N_NAME'] == 'SAUDI ARABIA']['N_NATIONKEY'].iloc[0]

# Filter suppliers in SAUDI ARABIA
saudi_suppliers = supplier[supplier['S_NATIONKEY'] == saudi_nation_key]

# Merge results to get supplier names
final_result = suppliers_df.merge(saudi_suppliers, left_on='S_SUPPKEY', right_on='S_SUPPKEY')

# Select required columns and sort
final_result = final_result[['NUMWAIT', 'S_NAME']].sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Output to CSV
final_result.to_csv('query_output.csv', index=False)
