import pandas as pd
import mysql.connector
from pymongo import MongoClient
import csv

# MySQL connection
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# Create cursors
mycursor = mydb.cursor()

# Execute the query and fetch the result into 'lineitem' and 'part' pandas dataframes
mycursor.execute("SELECT * FROM PART")
part = pd.DataFrame(mycursor.fetchall(), columns = [i[0] for i in mycursor.description])

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['tpch']
lineitem_collection = db['lineitem']

# Fetch all documents from 'lineitem' collection
lineitem = pd.DataFrame(list(lineitem_collection.find({}, {'_id': False})))

# Merge both pandas dataframes
df = pd.merge(part, lineitem, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Filter merged dataframe as per conditions given in SQL query
df = df[(
          (df['P_BRAND'] == 'Brand#12') & (df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) 
          & (df['L_QUANTITY'].between(1, 11)) & (df['P_SIZE'].between(1, 5))
          & (df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON') 
          ) 
         | 
         (
          (df['P_BRAND'] == 'Brand#23') & (df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']))
          & (df['L_QUANTITY'].between(10, 20)) & (df['P_SIZE'].between(1, 10))
          & (df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')  
         )
         |
         (
          (df['P_BRAND'] == 'Brand#34') & (df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']))
          & (df['L_QUANTITY'].between(20, 30)) & (df['P_SIZE'].between(1, 15))
          & (df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')   
         )]

# Calculate revenue
df['REVENUE'] = df['L_EXTENDEDPRICE']* (1 - df['L_DISCOUNT'])

# Write output to csv
df.to_csv('query_output.csv', index = False)
