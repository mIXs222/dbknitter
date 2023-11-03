import pymongo
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

# MongoDB Connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = client["tpch"]
collection_lineitem = mongo_db["lineitem"]
collection_part = mongo_db["part"]

# MySQL Connection
mydb = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)
mycursor = mydb.cursor()

# Query in MongoDB
query_lineitem = list(collection_lineitem.find({}, {"L_PARTKEY": 1, "L_EXTENDEDPRICE": 1, "L_QUANTITY": 1}))
query_part = list(collection_part.find({"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"}, {"P_PARTKEY": 1}))

# Convert the fetched data into DataFrame
df_lineitem = pd.DataFrame(query_lineitem)
df_part = pd.DataFrame(query_part)

# Merge both DataFrames on PARTKEY
merged_df = pd.merge(df_part, df_lineitem, left_on='P_PARTKEY', right_on='L_PARTKEY')

# SQL Query, we assume that lineitem table is also present in MySQL db according to the user assumption
mycursor.execute("SELECT AVG(L_QUANTITY) FROM lineitem")
avg_quantity = mycursor.fetchone()[0]

# Calculating the AVG_YEARLY in DataFrame
merged_df = merged_df[merged_df['L_QUANTITY'] < 0.2 * avg_quantity]
merged_df['AVG_YEARLY'] = merged_df['L_EXTENDEDPRICE'] / 7.0
result = merged_df[['AVG_YEARLY']].sum()

# Writing result to CSV file
result.to_csv('query_output.csv')
