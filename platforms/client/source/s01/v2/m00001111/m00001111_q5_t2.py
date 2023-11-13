import pandas as pd
import pymysql
import pymongo
from pymongo import MongoClient
from sqlalchemy import create_engine

# Connecting to MySQL
print("Connecting to MySQL")
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
supplier_df = pd.read_sql_table('supplier', mysql_conn)
nation_df = pd.read_sql_table('nation', mysql_conn)
region_df = pd.read_sql_table('region', mysql_conn)
mysql_conn.close()

# Connecting to MongoDB
print("Connecting to MongoDB")
mongodb_client = MongoClient("mongodb", 27017)
mongodb_db = mongodb_client["tpch"]
orders_df = pd.DataFrame(list(mongodb_db["orders"].find({}, {'_id': False})))
customer_df = pd.DataFrame(list(mongodb_db["customer"].find({}, {'_id': False})))
lineitem_df = pd.DataFrame(list(mongodb_db["lineitem"].find({}, {'_id': False})))

# Merging the dataframes
print("Merging the dataframes")
df = pd.merge(customer_df, orders_df, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = pd.merge(df, lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df = pd.merge(df, supplier_df, how='inner', left_on='C_NATIONKEY', right_on='S_NATIONKEY')
df = pd.merge(df, nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df = pd.merge(df, region_df, how='inner', left_on='N_REGIONKEY', right_on='R_REGIONKEY')

df = df[df['R_NAME'] == 'ASIA']
df = df[(df['O_ORDERDATE'] >= '1990-01-01') & (df['O_ORDERDATE'] < '1995-01-01')]

# Calculating the revenue
df["REVENUE"] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
df = df.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sorting the dataframe
df.sort_values('REVENUE', ascending=False, inplace=True)

# Export the dataframe to a csv
df.to_csv('query_output.csv', index=False)
