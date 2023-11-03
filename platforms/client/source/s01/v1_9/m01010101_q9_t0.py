from pymongo import MongoClient
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# mongodb connection
client = MongoClient("mongodb://localhost:27017/")
db_mongo = client["tpch"]
region = pd.DataFrame(list(db_mongo.region.find()))
supplier = pd.DataFrame(list(db_mongo.supplier.find()))
customer = pd.DataFrame(list(db_mongo.customer.find()))
lineitem = pd.DataFrame(list(db_mongo.lineitem.find()))

# mysql connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql', database='tpch')
cursor = cnx.cursor()

parts_query = "SELECT * FROM PART"
nation_query = "SELECT * FROM NATION"
partsupp_query = "SELECT * FROM PARTSUPP"
orders_query = "SELECT * FROM ORDERS"

part_df = pd.read_sql_query(parts_query, cnx)
nation_df = pd.read_sql_query(nation_query, cnx)
partsupp_df = pd.read_sql_query(partsupp_query, cnx)
orders_df = pd.read_sql_query(orders_query, cnx)

# merging all dataframes
df = pd.merge(part_df, supplier, how='inner', on='S_SUPPKEY')
df = pd.merge(df, lineitem, how='inner', on='L_PARTKEY')
df = pd.merge(df, partsupp_df, how='inner', on='PS_PARTKEY')
df = pd.merge(df, orders_df, how='inner', on='O_ORDERKEY')
df = pd.merge(df, nation_df, how='inner', on='N_NATIONKEY')

df['O_YEAR'] = pd.DatetimeIndex(df['O_ORDERDATE']).year
df['AMOUNT'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT']) - df['PS_SUPPLYCOST'] * df['L_QUANTITY']

# filter data where P_NAME LIKE '%dim%'
df = df[df['P_NAME'].str.contains('dim')]

# group by NATION and O_YEAR
grouped_df = df.groupby(['N_NAME', 'O_YEAR'])
grouped_df = grouped_df['AMOUNT'].sum().reset_index()
grouped_df.columns = ['NATION', 'O_YEAR', 'SUM_PROFIT']

# sort by NATION and O_YEAR
grouped_df = grouped_df.sort_values(['NATION', 'O_YEAR'], ascending=[True, False])

grouped_df.to_csv('query_output.csv', index=False)
