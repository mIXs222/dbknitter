import mysql.connector
import pymongo
import pandas as pd

# Connect to MySQL
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
mycursor = mydb.cursor()

# Retrieve parts from MySQL
mycursor.execute("SELECT * FROM PART WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'")
parts = pd.DataFrame(mycursor.fetchall(), columns=[i[0] for i in mycursor.description])

# Connect to MongoDB
myclient = pymongo.MongoClient("mongodb://mongodb:27017/")
mydb = myclient["tpch"]

# Retrieve lineitems from MongoDB
lineitems = pd.DataFrame(list(mydb["lineitem"].find()))

# Merge parts and lineitems
merged = pd.merge(parts, lineitems, how='inner', left_on='P_PARTKEY', right_on='L_PARTKEY')

# Calculate AVG_YEARLY
merged['AVG_QTY'] = merged.groupby(['P_PARTKEY'])['L_QUANTITY'].transform('mean')
merged['AVG_YEARLY'] = merged['L_EXTENDEDPRICE'] / 7.0
merged = merged[merged['L_QUANTITY'] < merged['AVG_QTY'] * 0.2]

# Write output to CSV
merged[['AVG_YEARLY']].to_csv('query_output.csv', index=False)
