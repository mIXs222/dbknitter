# import required packages
import pandas as pd
from pymongo import MongoClient
import mysql.connector as mysql

# MySQL connection
db1 = mysql.connect(
    host="mysql",
    user="root",
    passwd="my-secret-pw",
    database="tpch"
)

# Get Data from MySql
query = "SELECT * FROM PART"
df_part = pd.read_sql(query, db1)

... # repeat for all MySQL tables

# MongoDB connection
client = MongoClient("mongodb", 27017)
db2 = client['tpch']

# Get data from MongoDb
supplier = db2.supplier.find()
df_supplier = pd.DataFrame(list(supplier))

... # repeat for all MongoDB tables

# Merge all df_* dataframes using pd.merge() and Perform Calculations

# Write data to CSV
df.to_csv('query_output.csv')
