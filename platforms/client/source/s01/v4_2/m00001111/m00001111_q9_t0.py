import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient

# Connect to MySQL
db = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = db.cursor()

# MySQL query
mysql_query = "SELECT * FROM nation"
df_nation = pd.read_sql_query(mysql_query, db)

mysql_query = "SELECT * FROM part"
df_part = pd.read_sql_query(mysql_query, db)

mysql_query = "SELECT * FROM supplier"
df_supplier = pd.read_sql_query(mysql_query, db)

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# MongoDB query
partsupp = db.partsupp.find()
df_partsupp = pd.DataFrame(list(partsupp))

orders = db.orders.find()
df_orders = pd.DataFrame(list(orders))

lineitem = db.lineitem.find()
df_lineitem = pd.DataFrame(list(lineitem))

# Join all dataframes
df = pd.merge(df_lineitem, df_partsupp, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
df = pd.merge(df, df_part, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
df = pd.merge(df, df_orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df = pd.merge(df, df_supplier, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df = pd.merge(df, df_nation, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter data
df['O_YEAR'] = df['O_ORDERDATE'].apply(lambda x: x.year)
df = df[df['P_NAME'].str.contains('dim')]

# Generate AMOUNT column
df['AMOUNT'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT']) - df['PS_SUPPLYCOST'] * df['L_QUANTITY']

# Group by NATION and O_YEAR
df = df.groupby(['N_NAME', 'O_YEAR']).sum()

# Sort by NATION and O_YEAR
df = df.sort_values(['N_NAME', 'O_YEAR'], ascending=[True, False])

# Write output to CSV
df.to_csv('query_output.csv', columns=['N_NAME', 'O_YEAR', 'AMOUNT'])
