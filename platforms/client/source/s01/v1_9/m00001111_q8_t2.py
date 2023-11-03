import pymysql
import pymongo
from pymongo import MongoClient
import pandas as pd

# Fulfill MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)

# Fulfill MongoDB connection
client = MongoClient('mongodb', 27017)
mongo_conn = client.tpch

# MySQL Query
mysql_query = """
YOUR_SQL_QUERY_HERE
""" 
mysql_cursor.execute(mysql_query)
mysql_results = mysql_cursor.fetchall()

# MongoDB Query
mongo_query = {}
mongo_results = mongo_conn.YOUR_COLLECTION_NAME.find(mongo_query)

# Merge MySQL Query Result And MongoDB Query Result

mysql_df = pd.DataFrame(mysql_results)
mongo_df = pd.DataFrame(list(mongo_results))

df = pd.concat([mysql_df, mongo_df])

# Write the result to CSV
df.to_csv('query_output.csv', index=False, header=True)
