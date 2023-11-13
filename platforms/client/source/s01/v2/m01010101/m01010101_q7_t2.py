import pymongo
from pymongo import MongoClient
import pandas as pd
import pymysql

# MySQL Connection
mydb = pymysql.connect(
    host="mysql", 
    user="root", 
    passwd="my-secret-pw",
    db="tpch"
)

# MongoDB Connection
client = MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]

# Get the needed data from MySQL
mycursor = mydb.cursor()
mycursor.execute("SELECT * FROM nation, orders")
mysql_result = mycursor.fetchall()

# To make things easier convert it to DataFrame
mysql_df = pd.DataFrame(mysql_result, columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT', 'O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

# Get the needed data from MongoDB
supplier = mongodb.supplier
supplier_data = pd.DataFrame(list(supplier.find()))

customer = mongodb.customer
customer_data = pd.DataFrame(list(customer.find()))

lineitem = mongodb.lineitem
lineitem_data = pd.DataFrame(list(lineitem.find()))

# Merge the data together
result = pd.concat([supplier_data, customer_data, lineitem_data, mysql_df], axis=1, join="inner")

# Here you can select the needed columns and filter the data as needed, let's assume the new result is in new_result variable

# save the dataframe to csv
new_result.to_csv('query_output.csv')
