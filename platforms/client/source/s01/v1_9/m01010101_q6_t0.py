import pymysql
import pandas as pd
from pymongo import MongoClient
from pandas.io.json import json_normalize

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', 
                             port=3306, 
                             user='root', 
                             passwd='my-secret-pw', 
                             db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Read lineitem from MongoDB
cursor = db['lineitem'].find()
mongo_df = json_normalize(list(cursor))

# Execute query on MongoDB data
mask = (mongo_df['L_SHIPDATE'] >= '1994-01-01') & (mongo_df['L_SHIPDATE'] < '1995-01-01') & \
 (mongo_df['L_DISCOUNT'] > .06 - 0.01) & (mongo_df['L_DISCOUNT'] < .06 + 0.01) & (mongo_df['L_QUANTITY'] < 24)

filtered_data = mongo_df.loc[mask]

# Calculating the revenue
filtered_data['REVENUE'] = filtered_data['L_EXTENDEDPRICE'] * filtered_data['L_DISCOUNT']
result = filtered_data['REVENUE'].sum()

# Write result to CSV
result_df = pd.DataFrame([result], columns=['REVENUE'])
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
client.close()
