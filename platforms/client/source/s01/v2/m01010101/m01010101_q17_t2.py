import pandas as pd
import mysql.connector as sql
from pymongo import MongoClient

# MySQL connection
db_connection = sql.connect(host='mysql', database='tpch', user='root', password='my-secret-pw')
db_cursor = db_connection.cursor()

query = 'SELECT * FROM part'
db_cursor.execute(query)
table_rows = db_cursor.fetchall()
df_mysql = pd.DataFrame(table_rows, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

# MongoDB connection
client = MongoClient('mongodb', 27017)
db = client['tpch']
cursor = db.lineitem.find({})
df_mongo =  pd.DataFrame(list(cursor))
df_mongo = df_mongo.rename(columns={'_id': 'id'})

# Merge data from Mysql and Mongo DB
merged_df = pd.merge(df_mysql, df_mongo, left_on = 'P_PARTKEY', right_on = 'L_PARTKEY')

# Filter data
filtered_df = merged_df[(merged_df.P_BRAND == 'Brand#23') & (merged_df.P_CONTAINER == 'MED BAG')]

# Aggregation
aggregated_df = filtered_df.groupby('P_PARTKEY').agg({'L_QUANTITY':'mean'}).reset_index()
aggregated_df['avg_quantity'] = aggregated_df['L_QUANTITY'] * 0.2
filtered_df = pd.merge(filtered_df, aggregated_df[['P_PARTKEY', 'avg_quantity']], on = 'P_PARTKEY')
filtered_df = filtered_df[filtered_df.L_QUANTITY < filtered_df.avg_quantity]
result_df = filtered_df['L_EXTENDEDPRICE'].sum() / 7.0

# Write output to file
result_df.to_csv('query_output.csv')
