# import the necessary libraries 
import pandas as pd
import mysql.connector
import pymongo
from pymongo import MongoClient
from pandas import DataFrame

# Set up mysql connection
mysql_conn = mysql.connector.connect(
    host='mysql',
    database='tpch',
    user='root',
    password='my-secret-pw'
)
mysql_cursor = mysql_conn.cursor()

# perform selected queries for mysql part
nation_sql =  "SELECT * FROM nation"
supplier_sql = "SELECT * FROM supplier"
# execute query and assign to dataframe
mysql_cursor.execute(nation_sql)
nation = DataFrame(mysql_cursor.fetchall())
nation.columns = mysql_cursor.column_names
mysql_cursor.execute(supplier_sql)
supplier = DataFrame(mysql_cursor.fetchall())
supplier.columns = mysql_cursor.column_names

# Set up mongodb connection
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']
# get mongodb collections
orders = mdb['orders']
lineitem = mdb['lineitem']
# perform queries for mongodb and assign to dataframes
orders_df = DataFrame(list(orders.find()))
lineitem_df = DataFrame(list(lineitem.find()))

# merge datasets and execute query
df = pd.merge(pd.merge(supplier, nation, on='S_NATIONKEY'), pd.merge(orders_df, lineitem_df, on='L_ORDERKEY'))
query = df[df['O_ORDERSTATUS'] == 'F'][df['L_RECEIPTDATE'] > df['L_COMMITDATE']

query_size = query.groupby('S_NAME').size()
query_filtered = query_filtered.sort_values(by='S_NAME', ascending=True)
query_filtered['NUMWAIT'] = query_size.coerce_to_dataframe().sort_values(by='NUMWAIT', ascending=False)
query_filtered = query_filtered.drop(['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT', 'N_REGIONKEY', 'N_COMMENT'], axis=1)

# write to csv
query_filtered.to_csv('query_output.csv', index=False)
