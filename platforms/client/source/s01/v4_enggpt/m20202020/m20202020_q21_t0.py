import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Establish connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute query for MySQL tables supplier and lineitem
mysql_query = """
SELECT s.S_NAME, COUNT(*) as NUMWAIT
FROM supplier s
JOIN lineitem l1 ON s.S_SUPPKEY = l1.L_SUPPKEY
WHERE EXISTS (
    SELECT *
    FROM lineitem l2
    WHERE l1.L_ORDERKEY = l2.L_ORDERKEY
    AND l2.L_SUPPKEY <> s.S_SUPPKEY
) AND NOT EXISTS (
    SELECT *
    FROM lineitem l3
    WHERE l1.L_ORDERKEY = l3.L_ORDERKEY
    AND l3.L_SUPPKEY <> s.S_SUPPKEY
    AND l3.L_RECEIPTDATE > l3.L_COMMITDATE
) AND l1.L_RECEIPTDATE > l1.L_COMMITDATE
GROUP BY s.S_NAME
ORDER BY NUMWAIT DESC, s.S_NAME ASC;
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_result = cursor.fetchall()

# Convert MySQL result into DataFrame
mysql_df = pd.DataFrame(mysql_result, columns=['S_NAME', 'NUMWAIT'])

# Establish connection to Redis
redis_conn = DirectRedis(host='redis', port=6379)

# Get nation and orders data from Redis
df_nation = pd.read_json(redis_conn.get('nation'))
df_orders = pd.read_json(redis_conn.get('orders'))

# Filter orders with 'F' status and join with nation to get only Saudi Arabia suppliers
filtered_orders = df_orders[df_orders.O_ORDERSTATUS == 'F']
sa_nationkeys = df_nation[df_nation.N_NAME == 'SAUDI ARABIA']['N_NATIONKEY'].tolist()
sa_suppliers = mysql_df.merge(df_nation[df_nation['N_NATIONKEY'].isin(sa_nationkeys)],
                              left_on='N_NATIONKEY', right_on='N_NATIONKEY')

# Merge with filtered orders
final_result = sa_suppliers.merge(filtered_orders, left_on='L_SUPPKEY', right_on='O_ORDERKEY')

# Write the final result to a CSV file
final_result[['S_NAME', 'NUMWAIT']].to_csv('query_output.csv', index=False)
