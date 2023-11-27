# Import required libraries
import pandas as pd
import mysql.connector
from pymongo import MongoClient
import csv

# MySQL Connection 
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
mycursor = mydb.cursor()

# MySQL query
mycursor.execute("""
    SELECT N1.N_NAME as SUPP_NATION, N2.N_NAME as CUST_NATION 
    FROM nation N1, nation N2
    WHERE 
    (N1.N_NAME = 'JAPAN' and N2.N_NAME = 'INDIA') 
    or (N1.N_NAME = 'INDIA' and N2.N_NAME = 'JAPAN')
""")

mysql_data = mycursor.fetchall()

# MongoDB connection
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

# MongoDB queries
supplier = pd.DataFrame(list(mongodb.supplier.find()))
lineitem = pd.DataFrame(list(mongodb.lineitem.find()))
orders = pd.DataFrame(list(mongodb.orders.find()))
customer = pd.DataFrame(list(mongodb.customer.find()))

# Merge dataframes
df = pd.merge(supplier, lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
df = pd.merge(df, orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df = pd.merge(df, customer, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Filter data
df = df[(df['L_SHIPDATE'] >= '1995-01-01') & 
        (df['L_SHIPDATE'] <= '1996-12-31') & 
        ((df['SUPP_NATION'] == 'JAPAN') | (df['SUPP_NATION'] == 'INDIA')) &
        ((df['CUST_NATION'] == 'JAPAN') | (df['CUST_NATION'] == 'INDIA'))
       ]

# Generate revenue calculation
df['REVENUE'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])

# Group by SUPP_NATION, CUST_NATION, L_YEAR
df_grouped = df.groupby(['SUPP_NATION', 'CUST_NATION', df['L_SHIPDATE'].dt.year]).sum()

# Write results to CSV file
df_grouped.to_csv('query_output.csv')

