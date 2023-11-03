from pymongo import MongoClient
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# Establish MongoDB connection
mongodb_client = MongoClient('mongodb://localhost:27017/')
mongodb_db = mongodb_client['tpch']

# Execute MongoDB queries to get data
tables_mongodb = ['region', 'supplier', 'customer', 'lineitem']
data_mongodb = {table: pd.DataFrame(list(mongodb_db[table].find())) for table in tables_mongodb}

# Establish MySQL connection
mysql_conn = mysql.connector.connect(
  host="localhost",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
mysql_cursor = mysql_conn.cursor()

# Execute MySQL queries to get data
tables_mysql = ['NATION', 'PART', 'PARTSUPP', 'ORDERS']
data_mysql = {}
for table in tables_mysql:
    mysql_cursor.execute(f"SELECT * FROM {table}")
    data_mysql[table] = pd.DataFrame(mysql_cursor.fetchall(), columns=mysql_cursor.column_names)

# Merge MongoDB and MySQL data
data = {**data_mongodb, **data_mysql} 

# Create a Pandas DataFrame by joining the data
df = data['PART'].merge(data['PARTSUPP'], left_on='P_PARTKEY', right_on='PS_PARTKEY').merge(data['supplier'], left_on='PS_SUPPKEY', right_on='S_SUPPKEY').merge(data['NATION'], left_on='S_NATIONKEY', right_on='N_NATIONKEY').merge(data['region'], left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter data and execute the query
filtered_df = df[(df['P_SIZE'] == 15) & (df['P_TYPE'].str.contains('BRASS')) & (df['R_NAME'] == 'EUROPE') & (df['PS_SUPPLYCOST'] == df['PS_SUPPLYCOST'].min())]

# Sort and select columns
result_df = filtered_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])[['S_ACCTBAL','S_NAME','N_NAME','P_PARTKEY','P_MFGR','S_ADDRESS','S_PHONE','S_COMMENT']]

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
