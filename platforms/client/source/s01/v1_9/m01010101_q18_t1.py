from pymongo import MongoClient
import mysql.connector
import pandas as pd
import os

# MySQL connection
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mycursor = mydb.cursor()

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/") 
db = client["tpch"]

customer = pd.DataFrame(list(db.customer.find()))
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

# Merge MongoDB collections
merge1 = pd.merge(customer, orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged = pd.merge(merge1, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Create a sub dataframe to handle inner query
temp_df = lineitem.groupby('L_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
temp_df = temp_df[temp_df['L_QUANTITY'] > 300]
merged = merged[merged['O_ORDERKEY'].isin(temp_df['L_ORDERKEY'])]

# Finalize the query
result = merged.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({'L_QUANTITY': 'sum'}).reset_index()
result = result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Save to CSV
result.to_csv('query_output.csv', index=False)

