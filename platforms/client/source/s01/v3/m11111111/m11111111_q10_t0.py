# Required Libraries
import pandas as pd
import mysql.connector
from pymongo import MongoClient
from bson.objectid import ObjectId

# Connect to MySQL database
mydb = mysql.connector.connect(
  host="localhost",
  user="youruser",
  password="yourpassword",
  database="tpch"
)

# Get tables from MySQL database
mycursor = mydb.cursor()
mycursor.execute("SHOW TABLES")
tables = mycursor.fetchall()

# Connect to the MongoDB client
client = MongoClient('mongodb://localhost:27017/')
db = client['tpch']

# Get all documents from a table
def get_documents(table):
    collection = db[table]
    cursor = collection.find({})
    list_cur = list(cursor)
    df = pd.DataFrame(list_cur)
    return df

# Get data from MongoDB
df_nation = get_documents('nation')
df_customer = get_documents('customer')
df_orders = get_documents('orders')
df_lineitem = get_documents('lineitem')

# Join tables
merge1 = pd.merge(df_orders, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
merge2 = pd.merge(merge1, df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')
df_final = pd.merge(merge2, df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')

# Condition for O_ORDERDATE
df_final = df_final[(df_final['O_ORDERDATE'] >= '1993-10-01') & (df_final['O_ORDERDATE'] < '1994-01-01')]

# Group by
group_cols = ['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_MKTSEGMENT' , 'C_COMMENT']
df_final['REVENUE'] = df_final['L_EXTENDEDPRICE'] * (1 - df_final['L_DISCOUNT'])
df_grouped = df_final.groupby(group_cols , as_index = False)['REVENUE'].sum()

# Order by
df_sorted = df_grouped.sort_values(by='REVENUE', ascending=False)
df_sorted.to_csv('query_output.csv', index=False)
