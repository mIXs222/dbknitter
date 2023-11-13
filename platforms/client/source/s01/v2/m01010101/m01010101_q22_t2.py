import pymongo
import mysql.connector
import pandas as pd
from pandas.io.json import json_normalize

# MongoDB Connection
mongo_conn = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_conn["tpch"]
customer = mongo_db["customer"]

# MySQL Connection
mysql_conn = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

mysql_cursor = mysql_conn.cursor(buffered=True)

# Perform MySQL Query
mysql_cursor.execute("SELECT * FROM orders")
mysql_data = mysql_cursor.fetchall()
mysql_data = pd.DataFrame(mysql_data, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

# Perform MongoDB Query
mongo_data = customer.find()
mongo_data = pd.json_normalize(list(mongo_data))

# Merge the data
merged_data = pd.merge(mongo_data, mysql_data, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Perform the same calculations as in the user's SQL query
merged_data['CNTRYCODE'] = merged_data['C_PHONE'].str.slice(0, 2)
grouped = merged_data[merged_data['CNTRYCODE'].isin(['20', '40', '22', '30', '39', '42', '21']) & merged_data['C_ACCTBAL'] > merged_data[merged_data['C_ACCTBAL'] > 0.00]['C_ACCTBAL'].mean()].groupby('CNTRYCODE').agg(NUMCUST=('CNTRYCODE', 'count'), TOTACCTBAL=('C_ACCTBAL', 'sum')).reset_index()
grouped = grouped.sort_values('CNTRYCODE')

# Output to CSV
grouped.to_csv('query_output.csv', index=False)
