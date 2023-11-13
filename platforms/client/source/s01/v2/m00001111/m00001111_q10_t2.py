import pymongo
import pymysql
import pandas as pd
from pandas import DataFrame
from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
customer = db['customer']
orders = db['orders']
lineitem = db['lineitem']

# Convert MongoDB collections to pandas DataFrame
df_customer = DataFrame(list(customer.find()))
df_orders = DataFrame(list(orders.find()))
df_lineitem = DataFrame(list(lineitem.find()))

# Connect to MySQL
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

# Define the SQL query for nation
query = 'SELECT * FROM nation'
df_nation = pd.read_sql(query, con=connection)

# Close the MySQL connection
connection.close()

# Merge the data
df_result = pd.merge(df_customer, df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df_result = pd.merge(df_result, df_lineitem, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df_result = pd.merge(df_result, df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Filter the data
df_result = df_result[(df_result['O_ORDERDATE'] >= datetime(1993, 10, 1)) &
                      (df_result['O_ORDERDATE'] < datetime(1994, 1, 1)) &
                      (df_result['L_RETURNFLAG'] == 'R')]

# Add the REVENUE column
df_result['REVENUE'] = df_result['L_EXTENDEDPRICE'] * (1 - df_result['L_DISCOUNT'])

# Group the data
df_result = df_result.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'C_ADDRESS', 'C_COMMENT']).sum()

# Order the data
df_result = df_result.sort_values(['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False])

# Write to CSV
df_result.to_csv('query_output.csv')
