import mysql.connector
from pymongo import MongoClient
import pandas as pd

# MySQL connection
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# MongoDB connection
client = MongoClient('mongodb', 27017)
mongodb = client["tpch"]

# MySQL query
mycursor = mydb.cursor()
mycursor.execute("SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE FROM orders")
mysql_data = pd.DataFrame(mycursor.fetchall(), columns = ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])

# MongoDB queries
customer_data = pd.DataFrame(list(mongodb.customer.find()), columns = ['C_CUSTKEY', 'C_NAME'])
lineitem_data = pd.DataFrame(list(mongodb.lineitem.aggregate([{'$group' : {'_id' : '$L_ORDERKEY', 'L_QUANTITY' : {'$sum' : '$L_QUANTITY'}}}, {'$match': {'L_QUANTITY' : {'$gt' : 300}}}])))

# Combining the data
combined_df_1 = pd.merge(customer_data, mysql_data, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
combined_df_2 = pd.merge(combined_df_1, lineitem_data, how='inner', left_on='O_ORDERKEY', right_on='_id')

# Writing to csv
combined_df_2.to_csv('query_output.csv', index=False)
