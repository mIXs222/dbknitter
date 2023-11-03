import pandas as pd
import pymysql
import pymongo
from pymongo import MongoClient
from sqlalchemy import create_engine

# MySQL Connection
engine = create_engine("mysql+pymysql://root:my-secret-pw@mysql/tpch")

# fetching lineitem and supplier table from MySQL
query_lineItem = 'SELECT * FROM lineitem'
df_lineitem_mysql = pd.read_sql(query_lineItem, con=engine)

query_supplier = 'SELECT * FROM supplier'
df_supplier_mysql = pd.read_sql(query_supplier, con=engine)

# MongoDB connection
client = MongoClient("mongodb://mongodb:27017/")
database = client["tpch"]

# fetching lineitem and supplier data from MongoDB
cursor_lineitem = database["lineitem"].find()
df_lineitem_mongo = pd.DataFrame(list(cursor_lineitem))

cursor_supplier = database["supplier"].find()
df_supplier_mongo = pd.DataFrame(list(cursor_supplier))

# Concatenate MySQL and MongoDB data for lineitem and supplier
df_lineitem = pd.concat([df_lineitem_mongo, df_lineitem_mysql])
df_supplier = pd.concat([df_supplier_mongo, df_supplier_mysql])

# Filter & aggregate lineitem data
df_lineitem['REVENUE'] = df_lineitem['L_EXTENDEDPRICE'] * (1 - df_lineitem['L_DISCOUNT'])
df_lineitem['L_SHIPDATE'] = pd.to_datetime(df_lineitem['L_SHIPDATE'])
mask = (df_lineitem['L_SHIPDATE'] >= '1996-01-01') & (df_lineitem['L_SHIPDATE'] < '1996-04-01')
df_revenue = df_lineitem.loc[mask].groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()

# Max revenue
max_revenue = df_revenue['REVENUE'].max()

# Join supplier and revenue data
df_result = pd.merge(df_supplier, df_revenue, left_on = "S_SUPPKEY", right_on = "L_SUPPKEY")
df_result = df_result[df_result['REVENUE'] == max_revenue]

# Write output to CSV
df_result.to_csv("query_output.csv", index = False)
