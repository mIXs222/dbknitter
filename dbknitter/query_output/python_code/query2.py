import mysql.connector
from pymongo import MongoClient
import pandas as pd

# Create MySQL connection
mydb1 = mysql.connector.connect(
 host="localhost",
 user="root",
 passwd="mySecret",
 database="basicInfo"
)

mydb2 = mysql.connector.connect(
 host="localhost2",
 user="root",
 passwd="mySecret2",
 database="familyInfo"
)

# Create cursor
mycursor1 = mydb1.cursor()
mycursor2 = mydb2.cursor()

# Execute query
mycursor1.execute("SELECT NAME, AGE FROM table1")
mycursor2.execute("SELECT NAME FROM table2")

# Fetch all rows
mysql_data1 = mycursor1.fetchall()
mysql_data2 = mycursor2.fetchall()

# Convert to dataframe
df_mysql1 = pd.DataFrame(mysql_data1, columns=["NAME", "AGE"])
df_mysql2 = pd.DataFrame(mysql_data2, columns=["NAME"])

# Create MongoDB connection
client = MongoClient("mongodb://localhost3:27017/")
db = client["interestInfo"]

# Fetch all documents
mongodb_data = db.table3.find()

# Convert to dataframe
df_mongodb = pd.DataFrame(list(mongodb_data))

# Merge dataframes
result = pd.merge(df_mysql1, df_mongodb, on='NAME')
result = pd.merge(result, df_mysql2, on='NAME')

# Write to CSV
result.to_csv('query_output.csv', index=False)
