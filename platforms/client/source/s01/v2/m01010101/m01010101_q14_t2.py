import csv
import pandas as pd
import pymongo
from pymongo import MongoClient
import pymysql.cursors
import pandas.io.sql as psql

# Connect to the MySQL database
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

mysql_cursor = mysql_conn.cursor()

# fetch the required data from the part table
mysql_query = "SELECT * FROM part"
mysql_df = psql.read_sql(mysql_query, mysql_conn)

# Connect to the MongoDB client
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client.tpch

# fetch the required data from lineitem table
mongo_cursor = mongo_db['lineitem']
data = mongo_cursor.find({}, {'_id': 0})
mongo_df = pd.DataFrame(list(data))

# Merge data based on the common key L_PARTKEY = P_PARTKEY 
merged_df = pd.merge(mongo_df, mysql_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filter the data
filtered_merged_df = merged_df[(merged_df.L_SHIPDATE >= '1995-09-01') & (merged_df.L_SHIPDATE < '1995-10-01')]

# Calculate the required output
filtered_merged_df['PROMO_REVENUE'] = filtered_merged_df.apply(lambda row: 100.00 * row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']) if row['P_TYPE'].startswith('PROMO') else 0, axis=1)
promo_revenue = filtered_merged_df['PROMO_REVENUE'].sum() / filtered_merged_df['L_EXTENDEDPRICE'].sum() * (1 - filtered_merged_df['L_DISCOUNT'].sum())

# Write output to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["PROMO_REVENUE"])
    writer.writerow([promo_revenue])
