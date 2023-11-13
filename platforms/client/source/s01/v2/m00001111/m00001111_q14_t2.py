import mysql.connector
from pymongo import MongoClient
import pandas as pd
from pandasql import sqldf

# Connect to MySQL
mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Get data from mysql
mysql_cursor.execute("SELECT * FROM part")
mysql_rows = mysql_cursor.fetchall()

# Create a pandas dataframe
part_df = pd.DataFrame(mysql_rows, columns=[i[0] for i in mysql_cursor.description])

# Close connection
mysql_cursor.close()
mysql_conn.close()

# MongoDB connection
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Get data from MongoDB
lineitem_data = mongodb.lineitem.find()

# Create a pandas dataframe
lineitem_df = pd.DataFrame(list(lineitem_data))

# Ensure dates are in the correct format
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# SQL-style query
query = """
SELECT
    100.00 * SUM(CASE WHEN P_TYPE LIKE 'PROMO%' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT)
    ELSE 0
    END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE
FROM
    lineitem_df lineitem
JOIN
    part_df part
ON
    lineitem.L_PARTKEY = part.P_PARTKEY
WHERE
    lineitem.L_SHIPDATE >= '1995-09-01'
    AND lineitem.L_SHIPDATE < '1995-10-01'
"""

# Use pandasql to run the SQL-style query
results = sqldf(query)

# Write query's output to a CSV file 
results.to_csv('query_output.csv', index=False)
