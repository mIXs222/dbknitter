import pandas as pd
import numpy as np
import pymysql
import pymongo
from pymongo import MongoClient
from sqlalchemy import create_engine

# MYSQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB Connection
client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Pandas options
pd.set_option('display.expand_frame_repr', False)

# Get data in pandas dataframe
sql_query = "SELECT * FROM NATION"
df_mysql_nation = pd.read_sql_query(sql_query, mysql_conn)

sql_query = "SELECT * FROM PART"
df_mysql_part = pd.read_sql_query(sql_query, mysql_conn)

sql_query = "SELECT * FROM PARTSUPP"
df_mysql_partsupp = pd.read_sql_query(sql_query, mysql_conn)

sql_query = "SELECT * FROM ORDERS"
df_mysql_orders = pd.read_sql_query(sql_query, mysql_conn)

df_mongo_region = pd.DataFrame(list(db.region.find()))

df_mongo_supplier = pd.DataFrame(list(db.supplier.find()))

df_mongo_customer = pd.DataFrame(list(db.customer.find()))

df_mongo_lineitem = pd.DataFrame(list(db.lineitem.find()))

# Join dataframes and processes data
df_join = pd.merge(df_mysql_nation, df_mongo_supplier, how='inner', left_on='N_NATIONKEY', right_on='S_NATIONKEY')
df_join = pd.merge(df_join, df_mysql_orders, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df_join = pd.merge(df_join, df_mongo_customer, how='inner', left_on='C_CUSTKEY', right_on='C_CUSTKEY')
df_join = pd.merge(df_join, df_mongo_lineitem, how='inner', left_on='L_ORDERKEY', right_on='L_ORDERKEY')

df_join = df_join[(df_join['N1.N_NAME'] == 'JAPAN') & (df_join['N2.N_NAME'] == 'INDIA') | (df_join['N1.N_NAME'] == 'INDIA') & (df_join['N2.N_NAME'] == 'JAPAN')]
df_join = df_join[(df_join['L_SHIPDATE'] >= '1995-01-01') & (df_join['L_SHIPDATE'] <= '1996-12-31')]
df_join['VOLUME'] = df_join['L_EXTENDEDPRICE'] * (1 - df_join['L_DISCOUNT'])
df_join['YEAR'] = pd.DatetimeIndex(df_join['L_SHIPDATE']).year

output = df_join.groupby(['SUPP_NATION', 'CUST_NATION', 'YEAR'])['VOLUME'].sum().reset_index()

# Write to .csv
output.to_csv('query_output.csv', index=False)
