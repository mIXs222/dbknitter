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

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query to get nation and orders data from mysql
query_nation_orders = """
SELECT 
    n.N_NATIONKEY, 
    o.O_ORDERKEY
FROM 
    nation AS n
JOIN 
    orders AS o 
ON 
    n.N_NATIONKEY = o.O_CUSTKEY
WHERE 
    n.N_NAME = 'SAUDI ARABIA' 
AND 
    o.O_ORDERSTATUS = 'F';
"""

# Read nation and orders data into pandas dataframe
df_nation_orders = pd.read_sql(query_nation_orders, mysql_conn)

# Read supplier and lineitem data from redis into pandas dataframe
df_supplier = redis_conn.get('supplier')
df_lineitem = redis_conn.get('lineitem')

# Convert to DataFrame
df_supplier = pd.read_json(df_supplier)
df_lineitem = pd.read_json(df_lineitem)

# Filter suppliers located in Saudi Arabia
df_supplier_sa = df_supplier[df_supplier['S_NATIONKEY'].isin(df_nation_orders['N_NATIONKEY'])]

# Prepare a list of orders
orders_fulfilled = df_nation_orders['O_ORDERKEY'].tolist()

# Filter line items with L_RECEIPTDATE > L_COMMITDATE
df_lineitem_filtered = df_lineitem[
    (df_lineitem['L_ORDERKEY'].isin(orders_fulfilled)) & 
    (df_lineitem['L_RECEIPTDATE'] > df_lineitem['L_COMMITDATE'])
]

# Merge supplier and lineitem dataframes on supplier key
df_merged = df_lineitem_filtered.merge(df_supplier_sa, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Calculate waiting times for suppliers
waiting_time = df_merged.groupby('S_NAME').agg(NUMWAIT=('L_ORDERKEY', 'count')).reset_index()

# Order the results
ordered_results = waiting_time.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write to query_output.csv
ordered_results.to_csv('query_output.csv', index=False)

# Close all connections
mysql_conn.close()
redis_conn.close()
