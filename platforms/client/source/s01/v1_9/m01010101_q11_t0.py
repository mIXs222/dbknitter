import pandas as pd
from pymongo import MongoClient
import pymysql

# Establish connection with mongoDB
client = MongoClient("mongodb://localhost:27017/")
db_mongo = client["tpch"]

# Get data from mongoDB
supplier = pd.DataFrame(list(db_mongo.supplier.find()))
nation = pd.DataFrame(list(db_mongo.nation.find()))

# Establish connection with MySQL
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = conn.cursor()

# Execute query and fetch data from MySQL
query = "SELECT * FROM partsupp"
cursor.execute(query)
partsupp = pd.DataFrame(cursor.fetchall(), columns=['PS_PARTKEY','PS_SUPPKEY','PS_AVAILQTY','PS_SUPPLYCOST','PS_COMMENT'])

# Close MySQL connection
cursor.close()
conn.close()

# Carry out necessary transformations according to original query
merged_df = pd.merge(pd.merge(partsupp,supplier,left_on='PS_SUPPKEY', right_on='S_SUPPKEY'), nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

filtered_df = merged_df[merged_df['N_NAME']=='GERMANY']

grouped_df = filtered_df.groupby('PS_PARTKEY').agg({'PS_SUPPLYCOST':'sum'}).reset_index()

grouped_df['VALUE'] = grouped_df['PS_SUPPLYCOST']*grouped_df['PS_AVAILQTY']

grouped_df = grouped_df[grouped_df['VALUE'] > grouped_df['VALUE'].sum()*0.0001000000]

final_df = grouped_df.sort_values(by=['VALUE'], ascending=False)

# Save the results to CSV
final_df.to_csv('query_output.csv', index=False)
