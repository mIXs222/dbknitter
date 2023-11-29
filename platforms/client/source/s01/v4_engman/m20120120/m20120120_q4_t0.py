import pymysql
import pandas as pd
import csv
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch lineitem data from MySQL
lineitem_query = """
SELECT L_ORDERKEY, L_COMMITDATE, L_RECEIPTDATE
FROM lineitem
WHERE L_RECEIPTDATE > L_COMMITDATE AND L_SHIPDATE BETWEEN '1993-07-01' AND '1993-10-01';
"""
mysql_cursor.execute(lineitem_query)
late_lineitems = mysql_cursor.fetchall()

# Extract order keys from lineitem data
late_order_keys = {str(row[0]) for row in late_lineitems}

# Fetch orders data from Redis
orders = pd.DataFrame(redis_conn.get('orders'))
orders = orders[orders['O_ORDERKEY'].isin(late_order_keys)]

# Perform the aggregation and sorting
order_priority_counts = orders.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')
order_priority_counts.columns = ['O_ORDERPRIORITY', 'ORDER_COUNT']
order_priority_counts = order_priority_counts.sort_values(by='O_ORDERPRIORITY')

# Write the output to a csv file
order_priority_counts.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
