import pandas as pd
import mysql.connector
from pymongo import MongoClient
import csv

# Connect to MySQL
conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
cursor = conn.cursor()

# Fetch data from orders table
mysql_query_orders = 'SELECT * FROM orders'
cursor.execute(mysql_query_orders)
orders = cursor.fetchall()

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Fetch data from customer and lineitem collections
mongodb_query_customer = db.customer.find()
mongodb_query_lineitem = db.lineitem.find()

# Convert fetched data to dataframes
df_orders = pd.DataFrame(orders, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 
                                          'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

df_customer = pd.DataFrame(list(mongodb_query_customer))
df_lineitem = pd.DataFrame(list(mongodb_query_lineitem))

# Close connections
cursor.close()
conn.close()
client.close()

# Perform the required SQL operations on dataframes
lineitem_grouped = df_lineitem.groupby('L_ORDERKEY').sum()
df_lineitem_filtered = lineitem_grouped[lineitem_grouped['L_QUANTITY'] > 300]
df = pd.merge(df_customer, df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = pd.merge(df, df_lineitem_filtered, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Select required columns and sort the dataframe 
df = df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]
df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Write to csv
df.to_csv('query_output.csv', index=False)
