import pandas as pd
import pymongo
import pymysql

# Connect to MySQL Database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cur = mysql_conn.cursor()

# SQL Query to get data from 'supplier' table
mysql_query = "SELECT * FROM supplier"
mysql_cur.execute(mysql_query)
result = mysql_cur.fetchall()

supplier_df = pd.DataFrame(list(result), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Connect to MongoDB Database
mongo_conn = pymongo.MongoClient("mongodb://mongodb:27017/")
tpch_db = mongo_conn["tpch"]
lineitem = tpch_db["lineitem"]

lineitem_df = pd.DataFrame(list(lineitem.find()))

# Query data using pandas
revenue0 = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1996-01-01') & (lineitem_df['L_SHIPDATE'] < '1996-04-01')]
revenue0 = revenue0.groupby('L_SUPPKEY').agg({'TOTAL_REVENUE':'sum'}).reset_index()

output_df = pd.merge(supplier_df, revenue0, left_on='S_SUPPKEY', right_on='SUPPLIER_NO', how='inner')
max_revenue = revenue0['TOTAL_REVENUE'].max()
output_df = output_df[output_df['TOTAL_REVENUE'] == max_revenue]

# Write output to csv file
output_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongo_conn.close()
