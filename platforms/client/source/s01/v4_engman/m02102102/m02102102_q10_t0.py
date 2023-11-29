import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client['tpch']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for orders data
orders_query = """
SELECT 
    o.O_CUSTKEY,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue_lost
FROM
    orders o
INNER JOIN
    (SELECT * FROM lineitem WHERE L_RETURNFLAG = 'R') l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE
    o.O_ORDERDATE >= '1993-10-01' AND o.O_ORDERDATE < '1994-01-01'
GROUP BY
    o.O_CUSTKEY
"""
mysql_cursor.execute(orders_query)
orders_result = mysql_cursor.fetchall()

# Convert orders_result to DataFrame
orders_df = pd.DataFrame(list(orders_result), columns=['C_CUSTKEY', 'revenue_lost'])

# Retrieve customers from MongoDB
customers_data = mongodb_db.customer.find({})
customers_df = pd.DataFrame(list(customers_data))
# Keep only necessary columns
customers_df = customers_df[['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_ADDRESS', 'C_PHONE', 'C_NATIONKEY', 'C_COMMENT']]

# Retrieve nations from MySQL
nations_query = "SELECT * FROM nation"
mysql_cursor.execute(nations_query)
nations_result = mysql_cursor.fetchall()
nations_df = pd.DataFrame(list(nations_result), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

# Merge DataFrames
result_df = pd.merge(orders_df, customers_df, left_on='C_CUSTKEY', right_on='C_CUSTKEY')
result_df = pd.merge(result_df, nations_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Sort the merged result as required
result_df.sort_values(by=['revenue_lost', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False], inplace=True)

# Select and rename columns as per query output
final_df = result_df[['C_CUSTKEY', 'C_NAME', 'revenue_lost', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]

# Write the DataFrame to a CSV file
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
