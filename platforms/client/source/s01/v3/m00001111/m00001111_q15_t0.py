import mysql.connector
import pymongo
import pandas as pd
import os 
from datetime import datetime, timedelta

# Establish connection to mysql database
mysql_conn = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
    )

# Establish connection to mongodb database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Fetch data from mysql
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier")
supplier_data = mysql_cursor.fetchall()

# Transform data into pandas dataframe
mysql_df = pd.DataFrame(supplier_data, columns=["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE"])

# Fetch data from mongodb
lineitem_cursor = mongodb.lineitem.find({
    "L_SHIPDATE": {
        "$gte": datetime.strptime('1996-01-01', '%Y-%m-%d'), 
        "$lt": datetime.strptime('1996-01-01', '%Y-%m-%d') + timedelta(3*365/12)
        }
    })

# Transform data into pandas dataframe
lineitem_data = pd.DataFrame(list(lineitem_cursor))

# Calculate total revenue
lineitem_data['TOTAL_REVENUE'] = lineitem_data['L_EXTENDEDPRICE'] * (1 - lineitem_data['L_DISCOUNT'])

# Group by SUPPKEY
revenue0 = lineitem_data.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Merge two dataframes
result_df = mysql_df.merge(revenue0, how='inner', left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Filter out rows
max_revenue = revenue0['TOTAL_REVENUE'].max()
result_df = result_df[result_df['TOTAL_REVENUE'] == max_revenue]

# Sort by S_SUPPKEY
result_df = result_df.sort_values(by=['S_SUPPKEY'])

# Save output to csv
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongo_client.close()
