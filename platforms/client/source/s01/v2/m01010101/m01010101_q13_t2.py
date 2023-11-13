import pandas as pd
import mysql.connector
from pymongo import MongoClient
import csv

# Connect to MySQL
mysql_db = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# Get MySQL data
mysql_cursor = mysql_db.cursor()
mysql_cursor.execute("SELECT * FROM orders WHERE O_COMMENT NOT LIKE '%pending%deposits%'")
mysql_data = mysql_cursor.fetchall()
mysql_df = pd.DataFrame(mysql_data, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

# Connect to MongoDB
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Get MongoDB data
mongo_data = mongo_db["customer"].find()
mongo_df = pd.DataFrame(list(mongo_data))

# Merge MySQL and MongoDB data on C_CUSTKEY = O_CUSTKEY
merged_df = pd.merge(mongo_df, mysql_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Perform group by operations
grouped_df = merged_df.groupby('C_CUSTKEY').agg({
    'O_ORDERKEY': 'count'
}).rename(columns={'O_ORDERKEY': 'C_COUNT'})

grouped_df = grouped_df.groupby('C_COUNT').agg({
    'C_COUNT': 'count'
}).rename(columns={'C_COUNT': 'CUSTDIST'})

# Order by CUSTDIST DESC, C_COUNT DESC
ordered_df = grouped_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write output to CSV
ordered_df.to_csv("query_output.csv")
