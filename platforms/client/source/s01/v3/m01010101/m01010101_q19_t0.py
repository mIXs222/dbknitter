import mysql.connector
import pymongo
import pandas as pd

# Connect to MySQL server
mysql_conn = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("SELECT * FROM part")
mysql_data = mysql_cursor.fetchall()

# Convert MySQL data to pandas df
mysql_df = pd.DataFrame(mysql_data, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

# Connect to MongoDB server
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_col = mongo_db["lineitem"]

# Get all documents from the collection
mongo_data = list(mongo_col.find())

# Convert MongoDB data to pandas df
mongo_df = pd.DataFrame(mongo_data)

# Merge both data on P_PARTKEY and L_PARTKEY columns
df = pd.merge(mongo_df, mysql_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Implement the query logic on the df
df = df[((df['P_BRAND'] == 'Brand#12') & (df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & df['L_QUANTITY'].between(1, 11) & df['P_SIZE'].between(1, 5) & df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) & (df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |
        ((df['P_BRAND'] == 'Brand#23') & (df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & df['L_QUANTITY'].between(10, 20) & df['P_SIZE'].between(1, 10) & df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) & (df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |
        ((df['P_BRAND'] == 'Brand#34') & (df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & df['L_QUANTITY'].between(20, 30) & df['P_SIZE'].between(1, 15) & df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) & (df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))]

# Calculate the REVENUE
df['REVENUE'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
df = df[['REVENUE']]

# Write the output to csv file 
df.to_csv('query_output.csv', index=False)
