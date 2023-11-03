import mysql.connector
from pymongo import MongoClient
import pandas as pd
import numpy as np

# Connect to MySQL server
mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql',
                              database='tpch')

# Get the part data
mysql_df = pd.read_sql('SELECT * FROM PART', con=mysql_conn)
mysql_conn.close()

# Connect to MongoDB server
mongo_conn = MongoClient('mongodb', 27017)
db = mongo_conn['tpch']

# Get the lineitem data
mongo_df = pd.DataFrame(list(db.lineitem.find()))

# Merge both dataframes based on L_PARTKEY and P_PARTKEY
merged_df = pd.merge(mongo_df, mysql_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filter the data based on the date
merged_df['L_SHIPDATE'] = pd.to_datetime(merged_df['L_SHIPDATE'])
mask = (merged_df['L_SHIPDATE'] >= '1995-09-01') & (merged_df['L_SHIPDATE'] < '1995-10-01')
filtered_df = merged_df.loc[mask]

# Calculate the required output
filtered_df['CALCULATED_VALUE'] = np.where(filtered_df['P_TYPE'].str.contains('PROMO'), filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT']), 0)
filtered_df['PROMO_REVENUE'] = 100.00 * filtered_df['CALCULATED_VALUE'].sum() / (filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])).sum()

# Write the output to the file
filtered_df[['PROMO_REVENUE']].to_csv('query_output.csv', index=False)
