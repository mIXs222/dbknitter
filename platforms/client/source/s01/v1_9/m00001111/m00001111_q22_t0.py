from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

hostname = "mysql"
database = "tpch"
username = "root"
password = "my-secret-pw"

# Create a connection with MySQL
engine = create_engine(f"mysql+pymysql://{username}:{password}@{hostname}/{database}")
connection = engine.connect()

# Fetch data from Customer and Order
query_customer = "SELECT * FROM customer"
query_orders = "SELECT * FROM orders"
df_customer = pd.read_sql(query_customer, connection)
df_orders = pd.read_sql(query_orders, connection)

# Close the MySQL connection
connection.close()

# Connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client["tpch"]

# Fetch Data from MongoDB
df_partsupp = pd.DataFrame(list(db.partsupp.find()))
df_lineitem = pd.DataFrame(list(db.lineitem.find()))

cols_customer = list(df_customer.columns)
cols_orders = list(df_orders.columns)
cols_partsupp= list(df_partsupp.columns)
cols_lineitem= list(df_lineitem.columns)

df_customer.C_PHONE=df_customer.C_PHONE.astype(str)

df_customer['CNTRYCODE'] = df_customer['C_PHONE'].str.slice(start=0, stop=2)
df_customer = df_customer[df_customer.CNTRYCODE.isin(['20', '40', '22', '30', '39', '42', '21'])]
df_customer = df_customer[df_customer.C_ACCTBAL > df_customer.C_ACCTBAL[df_customer.C_ACCTBAL > 0].mean()]
df_customer = df_customer[~df_customer.C_CUSTKEY.isin(df_orders.O_CUSTKEY)]

df_customer['C_ACCTBAL'] = df_customer['C_ACCTBAL']
df_group = df_customer.groupby('CNTRYCODE').agg(NUMCUST = pd.NamedAgg(column = 'C_CUSTKEY', aggfunc = 'count'),
                    TOTACCTBAL = pd.NamedAgg(column = 'C_ACCTBAL', aggfunc = 'sum')).reset_index()

df_group.to_csv('query_output.csv', index=False)
