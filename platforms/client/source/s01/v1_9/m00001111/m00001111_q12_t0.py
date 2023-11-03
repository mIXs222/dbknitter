import pymysql
import pymongo
from pymongo import MongoClient
import pandas as pd

# MySQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)

# MongoDB Connection
mongo_client = MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# SQL Query
sql_query = "SELECT * FROM orders;"
cursor.execute(sql_query)
orders = pd.DataFrame(cursor.fetchall())

# MongoDB Query
lineitem = pd.DataFrame(list(mongo_db.lineitem.find()))

# Merge the two dataframes on O_ORDERKEY and L_ORDERKEY
merged = pd.merge(orders, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Apply the conditions and grouping specified in the query
output = merged[
    (merged['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) & 
    (merged['L_COMMITDATE'] < merged['L_RECEIPTDATE']) &
    (merged['L_SHIPDATE'] < merged['L_COMMITDATE']) &
    (merged['L_RECEIPTDATE'] >= '1994-01-01') &
    (merged['L_RECEIPTDATE'] < '1995-01-01')
].groupby('L_SHIPMODE').apply(lambda group: pd.Series({
    'HIGH_LINE_COUNT': sum((group['O_ORDERPRIORITY'] == '1-URGENT') | (group['O_ORDERPRIORITY'] == '2-HIGH')),
    'LOW_LINE_COUNT': sum((group['O_ORDERPRIORITY'] != '1-URGENT') & (group['O_ORDERPRIORITY'] != '2-HIGH'))
})).reset_index()

# Writing the output to the csv file
output.to_csv('query_output.csv', index=False)
