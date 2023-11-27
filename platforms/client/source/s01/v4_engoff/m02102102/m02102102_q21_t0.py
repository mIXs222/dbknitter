import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

mysql_cursor = mysql_conn.cursor()

# Get all suppliers from SAUDI ARABIA
sql_query = """
SELECT s.S_SUPPKEY, s.S_NAME
FROM supplier s
JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE n.N_NAME = 'SAUDI ARABIA'
"""
mysql_cursor.execute(sql_query)
suppliers = pd.DataFrame(mysql_cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME'])

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to Redis and get the lineitem table
redis_conn = DirectRedis(host='redis', port=6379, db=0)
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Filter multi-supplier orders with status 'F' where only one supplier failed to meet the commitment
filtered_lineitems = lineitem_df[
    (lineitem_df['L_RETURNFLAG'] == 'F') &
    (lineitem_df.groupby('L_ORDERKEY')['L_SUPPKEY'].transform('nunique') > 1) &
    (lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE'])
]

# Get suppliers who kept orders waiting
waiting_suppliers = suppliers[suppliers['S_SUPPKEY'].isin(filtered_lineitems['L_SUPPKEY'])]

# Write the result to a .csv file
waiting_suppliers.to_csv('query_output.csv', index=False)
