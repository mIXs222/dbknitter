import mysql.connector
import pymongo
from pymongo import MongoClient
import pandas as pd

# MySQL connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw',host='mysql',database='tpch')
cursor = cnx.cursor()

# execute query and get partsupp and nation data from mysql
query_mysql = ("SELECT N.N_NATIONKEY, N.N_NAME, P.PS_PARTKEY, P.PS_SUPPKEY, P.PS_AVAILQTY, P.PS_SUPPLYCOST FROM nation N, partsupp P WHERE P.PS_SUPPKEY = N.N_NATIONKEY AND N.N_NAME = 'GERMANY'")
cursor.execute(query_mysql)

# put the result into a pandas dataframe
result_mysql = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST'])
cnx.close()

# MongoDB connection
client = MongoClient("mongodb", 27017)
db = client.tpch

# get supplier data from mongodb
cur = db.supplier.find()
result_mongo = pd.DataFrame(list(cur))

# Merge the results
merged_df = pd.merge(result_mysql, result_mongo, left_on='N_NATIONKEY', right_on='S_NATIONKEY')

# Execute the main query
main_query_result = merged_df.groupby('PS_PARTKEY').apply(lambda x: (x['PS_SUPPLYCOST'] * x['PS_AVAILQTY']).sum() > (merged_df['PS_SUPPLYCOST'] * merged_df['PS_AVAILQTY']).sum() * 0.0001000000).reset_index(name='Value')

# Sort and write to csv file
main_query_result_sorted = main_query_result.sort_values('Value', ascending=False)
main_query_result_sorted.to_csv('query_output.csv', index=False)
