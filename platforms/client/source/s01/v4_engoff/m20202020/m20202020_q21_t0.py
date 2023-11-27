import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connecting to MySQL tpch database
my_conn = pymysql.connect(host='mysql', 
                          user='root', 
                          password='my-secret-pw', 
                          db='tpch')

# Get the supplier and lineitem tables from MySQL
supplier_query = 'SELECT * FROM supplier WHERE S_NATIONKEY = (SELECT N_NATIONKEY FROM nation WHERE N_NAME = "SAUDI ARABIA");'
lineitem_query = 'SELECT * FROM lineitem WHERE L_LINESTATUS = "F";'
supplier_df = pd.read_sql(supplier_query, my_conn)
lineitem_df = pd.read_sql(lineitem_query, my_conn)

# Closing MySQL connection
my_conn.close()

# Connect to Redis and get nation and orders tables
redis_conn = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_conn.get('nation')) # Assuming the table in Redis is stored in JSON format
orders_df = pd.read_json(redis_conn.get('orders')) # Assuming the table in Redis is stored in JSON format

# Filtering the nation dataframe for 'SAUDI ARABIA'
nation_df_sa = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']

# Join supplier with nation on N_NATIONKEY == S_NATIONKEY
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df_sa['N_NATIONKEY'])]

# Join lineitem with orders on L_ORDERKEY == O_ORDERKEY
multi_supplier_orders = orders_df[orders_df['O_ORDERSTATUS'] == 'F']
lineitem_df = lineitem_df[lineitem_df['L_ORDERKEY'].isin(multi_supplier_orders['O_ORDERKEY'])]

# Find the orders where they were the only supplier who failed to meet the committed delivery date.
# Filtering for lineitems with a receipt date after the commit date
late_lineitems = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']]

# Join the late_lineitems with multi_supplier_orders to find relevant supplier keys
relevant_orders = late_lineitems[late_lineitems['L_ORDERKEY'].isin(multi_supplier_orders['O_ORDERKEY'])]
relevant_supplier_keys = relevant_orders['L_SUPPKEY'].unique()

# Finding the suppliers who kept orders waiting
suppliers_who_kept_waiting = supplier_df[supplier_df['S_SUPPKEY'].isin(relevant_supplier_keys)]

# Exporting the results to a CSV file
suppliers_who_kept_waiting.to_csv('query_output.csv', index=False)
