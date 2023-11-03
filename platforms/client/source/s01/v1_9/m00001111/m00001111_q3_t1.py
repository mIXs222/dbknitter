import pandas as pd
from pymongo import MongoClient
import mysql.connector
from pandas.io.json import json_normalize

# MySQL connection
mydb = mysql.connector.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mycursor = mydb.cursor()

# MongoDB connection
client = MongoClient('mongodb://mongodb:27017/')
db = client.tpch

# Fetch data from MySQL
mycursor.execute("SELECT * FROM NATION")
nation = pd.DataFrame(mycursor.fetchall(), columns=['N_NATIONKEY','N_NAME','N_REGIONKEY','N_COMMENT'])

mycursor.execute("SELECT * FROM REGION")
region = pd.DataFrame(mycursor.fetchall(), columns=['R_REGIONKEY','R_NAME','R_COMMENT'])

mycursor.execute("SELECT * FROM PART")
part = pd.DataFrame(mycursor.fetchall(), columns=['P_PARTKEY','P_NAME','P_MFGR','P_BRAND','P_TYPE','P_SIZE','P_CONTAINER','P_RETAILPRICE','P_COMMENT'])

mycursor.execute("SELECT * FROM SUPPLIER")
supplier = pd.DataFrame(mycursor.fetchall(), columns=['S_SUPPKEY','S_NAME','S_ADDRESS','S_NATIONKEY','S_PHONE','S_ACCTBAL','S_COMMENT'])

# Fetch data from MongoDB
partsupp = json_normalize(list(db.partsupp.find()))
customer = json_normalize(list(db.customer.find()))
orders = json_normalize(list(db.orders.find()))
lineitem = json_normalize(list(db.lineitem.find()))

# Merge the dataframes based on the query in SQL, then apply the required transformations
df = pd.merge(customer, orders, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = pd.merge(df, lineitem, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df = df[df['C_MKTSEGMENT'] == 'BUILDING']
df = df[df['O_ORDERDATE'] < '1995-03-15']
df = df[df['L_SHIPDATE'] > '1995-03-15']

df['REVENUE'] =  df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])

df = df[['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]

df = df.sort_values(['REVENUE', 'O_ORDERDATE'], ascending=[False, True])
df.to_csv('query_output.csv', index=False)
