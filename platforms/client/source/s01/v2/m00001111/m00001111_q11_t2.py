import pandas as pd
import mysql.connector
import pymongo

# Connect to MySQL
mydb = mysql.connector.connect(user='root', password='my-secret-pw',
                               host='mysql', database='tpch')
mycursor = mydb.cursor()

# Query MySQL
mycursor.execute("SELECT * FROM supplier, nation WHERE S_NATIONKEY = N_NATIONKEY")
mysql_data = mycursor.fetchall()
mysql_df = pd.DataFrame(mysql_data, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT', 'N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

# Connect to MongoDB
myclient = pymongo.MongoClient("mongodb:27017")
mydb = myclient["tpch"]
mycol = mydb["partsupp"]

# Query MongoDB
mongodb_data = mycol.find()
mongodb_df = pd.DataFrame(list(mongodb_data))

# Merge data
merged_df = pd.merge(mysql_df, mongodb_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY', how='inner')
merged_df = merged_df[merged_df['N_NAME'] == 'GERMANY']

# Execute query
merged_df['VALUE'] = merged_df['PS_SUPPLYCOST']*merged_df['PS_AVAILQTY']
result_df = merged_df.groupby('PS_PARTKEY')['VALUE'].sum().reset_index()
result_df = result_df[result_df['VALUE'] > result_df['VALUE'].sum()*0.0001000000]
result_df = result_df.sort_values('VALUE', ascending=False)

# Save result
result_df.to_csv('query_output.csv', index=False)
