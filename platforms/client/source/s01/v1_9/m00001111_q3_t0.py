import mysql.connector
import pymongo
from pymongo import MongoClient
import pandas as pd

# Connect to MySQL
mysql_db = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
mysql_cursor = mysql_db.cursor()

# Execute SQL query on MySQL
mysql_cursor.execute('SELECT * FROM customer')
customers = pd.DataFrame(mysql_cursor.fetchall(), columns=[column[0] for column in mysql_cursor.description])

mysql_cursor.execute('SELECT * FROM orders')
orders = pd.DataFrame(mysql_cursor.fetchall(), columns=[column[0] for column in mysql_cursor.description])

# Close MySQL connection
mysql_cursor.close()
mysql_db.close()

# Connect to MongoDB
client = MongoClient("mongodb://mongodb:27017/")
mongodb = client['tpch']

# Query MongoDB database
lineitems_records = mongodb.lineitem.find()

# Convert MongoDB data to DataFrame
lineitems = pd.DataFrame(list(lineitems_records))

# Merge, filter and group data with pandas
cond1 = lineitems['L_SHIPDATE'] > '1995-03-15'
data = pd.merge(customers, orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
data = pd.merge(data, lineitems[cond1], left_on="O_ORDERKEY", right_on="L_ORDERKEY")
data = data[data['C_MKTSEGMENT']=='BUILDING']
data = data[data['O_ORDERDATE'] < '1995-03-15']
data['REVENUE'] = data['L_EXTENDEDPRICE'] * (1 - data['L_DISCOUNT'])
grouped = data.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])['REVENUE'].sum().reset_index()

# Sort data and write to csv
result = grouped.sort_values(['REVENUE', 'O_ORDERDATE'], ascending=[False, True])
result.to_csv('query_output.csv', index=False)
