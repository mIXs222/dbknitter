import mysql.connector
from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# Connect to MySQL
mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = MongoClient('mongodb:27017')
mongodb_database = mongodb_client['tpch']

# MySQL query
mysql_query = """
SELECT
    N_NAME AS SUPP_NATION,
    S_ACCTBAL
FROM
    nation,
    supplier
WHERE
    S_NATIONKEY = N_NATIONKEY
AND (N_NAME = 'JAPAN' OR N_NAME = 'INDIA')
"""
mysql_cursor.execute(mysql_query)
mysql_data = mysql_cursor.fetchall()

# Transfer MySQL data to a pandas DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=['SUPP_NATION', 'S_ACCTBAL'])

# MongoDB query
mongo_data = mongodb_database.customer.find({
    'C_NATIONKEY': {'$in': ['JAPAN', 'INDIA']},
    'O_ORDERDATE': {'$gt': datetime(1995, 1, 1), '$lt': datetime(1996, 12, 31)}
}, {'C_NATIONKEY': 1, 'C_ACCTBAL': 1, '_id': 0})

# Transfer MongoDB data to a pandas DataFrame
mongo_df =  pd.DataFrame(list(mongo_data))

# Merge DataFrames
result = pd.merge(mysql_df, mongo_df, left_on='SUPP_NATION', right_on='C_NATIONKEY')

# Write to CSV
result.to_csv('query_output.csv', index=False)
