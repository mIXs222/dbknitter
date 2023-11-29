import pymysql
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connecting to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# MySQL query to find nation key for 'SAUDI ARABIA'
nation_query = """
SELECT N_NATIONKEY
FROM nation
WHERE N_NAME = 'SAUDI ARABIA';
"""

# Execute the nation query
with mysql_conn.cursor() as cursor:
    cursor.execute(nation_query)
    nation_result = cursor.fetchone()
    saudi_nationkey = nation_result[0]

# MySQL query to find orders with status 'F'
orders_query = """
SELECT O_ORDERKEY
FROM orders
WHERE O_ORDERSTATUS = 'F';
"""

# Execute the orders query
with mysql_conn.cursor() as cursor:
    cursor.execute(orders_query)
    orders_result = cursor.fetchall()
    failing_orderkeys = [order[0] for order in orders_result]

# Close MySQL connection
mysql_conn.close()

# Connecting to Redis and retrieve supplier and lineitem DataFrames
redis_conn = DirectRedis(host='redis', port=6379, db=0)
supplier_df = pd.read_msgpack(redis_conn.get('supplier'))
lineitem_df = pd.read_msgpack(redis_conn.get('lineitem'))

# Close Redis connection
redis_conn.connection_pool.disconnect()

# Filter suppliers based on the nation key
saudi_suppliers_df = supplier_df[supplier_df['S_NATIONKEY'] == saudi_nationkey]

# Filter lineitems based on failing orders
failing_lineitems_df = lineitem_df[lineitem_df['L_ORDERKEY'].isin(failing_orderkeys)]

# Find Multi-Supplier orders
multi_supplier_orders = failing_lineitems_df.groupby('L_ORDERKEY').filter(lambda x: x['S_SUPPKEY'].nunique() > 1)

# Find the suppliers who were the only one to fail the delivery
def check_only_supplier_failed(group):
    return (group['L_COMMITDATE'] < group['L_RECEIPTDATE']).all()

single_failed_suppliers = multi_supplier_orders.groupby('L_ORDERKEY').filter(check_only_supplier_failed)

# Count the number of times each supplier caused waiting
supplier_wait_counts = single_failed_suppliers.groupby('L_SUPPKEY').size().reset_index(name='NUMWAIT')

# Join with the supplier names
output_df = supplier_wait_counts.merge(saudi_suppliers_df[['S_SUPPKEY', 'S_NAME']], left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Order the result
ordered_output_df = output_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write result to CSV
ordered_output_df[['NUMWAIT', 'S_NAME']].to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
