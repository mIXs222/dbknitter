from pymongo import MongoClient
from mysql.connector import connect, Error
import pandas as pd

# Connect to MySQL
mysql_conn = connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
mysql_cur = mysql_conn.cursor()

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client.tpch

# Get supplier, orders, nation datasets from MySQL
supplier_query = "SELECT * FROM supplier"
orders_query = "SELECT * FROM orders"
nation_query = "SELECT * FROM nation"

mysql_cur.execute(supplier_query)
suppliers = pd.DataFrame(mysql_cur.fetchall(), columns=[desc[0] for desc in mysql_cur.description])

mysql_cur.execute(orders_query)
orders = pd.DataFrame(mysql_cur.fetchall(), columns=[desc[0] for desc in mysql_cur.description])

mysql_cur.execute(nation_query)
nations = pd.DataFrame(mysql_cur.fetchall(), columns=[desc[0] for desc in mysql_cur.description])

# Get lineitem dataset from MongoDB
lineitems = pd.DataFrame(list(mongodb.lineitem.find()))

# Close MySQL Connection
mysql_cur.close()
mysql_conn.close()

# Close MongoDB Connection
client.close()

# Merge datasets and perform operations
results = suppliers.merge(nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
results = results.merge(lineitems, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
results = results.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
results = results[(results['O_ORDERSTATUS'] == 'F') & (results['L_RECEIPTDATE'] > results['L_COMMITDATE']) & (results['N_NAME'] == 'SAUDI ARABIA')]
results = results.groupby('S_NAME').size().reset_index(name='NUMWAIT')
results = results.sort_values(['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write results to csv
results.to_csv('query_output.csv', index=False)
