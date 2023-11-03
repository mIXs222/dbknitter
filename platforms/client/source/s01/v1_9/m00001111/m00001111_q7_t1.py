import pandas as pd
import pymysql
import pymongo
from datetime import datetime as dt
from pymongo import MongoClient
import csv

# Connect to MySQL
mydb = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pq",
    database="tpch"
)

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017")
mdb = client["tpch"]

# Read customers from MongoDB
customer_cursor = mdb.customer.find({})
customer_df = pd.DataFrame(list(customer_cursor))

# Read lineitems from MongoDB
lineitem_cursor = mdb.lineitem.find({})
lineitem_df = pd.DataFrame(list(lineitem_cursor))

# Read orders from MongoDB
orders_cursor = mdb.orders.find({})
orders_df = pd.DataFrame(list(orders_cursor))

# Read suppliers from MySQL
supplier_df = pd.read_sql('SELECT * FROM SUPPLIER', con=mydb)

# Read nation from MySQL
nation_df = pd.read_sql('SELECT * FROM NATION', con=mydb)

# Query Execution
filtered_data = pd.merge(supplier_df, lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
filtered_data = pd.merge(filtered_data, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
filtered_data = pd.merge(filtered_data, customer_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
filtered_data = pd.merge(filtered_data, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
filtered_data.rename(columns = {'N_NAME':'SUPP_NATION'}, inplace = True)
filtered_data = pd.merge(filtered_data, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
filtered_data.rename(columns = {'N_NAME':'CUST_NATION'}, inplace = True)
filtered_data = filtered_data[(filtered_data['L_SHIPDATE'] > dt.strptime('1995-01-01', '%Y-%m-%d')) & (filtered_data['L_SHIPDATE'] < dt.strptime('1996-12-31', '%Y-%m-%d'))]

filtered_data['VOLUME'] = filtered_data['L_EXTENDEDPRICE'] * (1 - filtered_data['L_DISCOUNT'])
filtered_data = filtered_data.groupby(['SUPP_NATION', 'CUST_NATION', filtered_data.L_SHIPDATE.dt.year, 'VOLUME']).sum()
filtered_data.reset_index().to_csv('query_output.csv', index=False)
