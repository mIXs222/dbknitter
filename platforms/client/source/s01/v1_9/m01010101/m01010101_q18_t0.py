import mysql.connector
import pymongo
import csv
import pandas as pd

# MySQL Connection
mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Mongo Connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client["tpch"]

# Extract data from MongoDB lineitem
query1 = mongo_db.lineitem.aggregate([
    {"$group": {"_id": "$L_ORDERKEY", "totalQ": {"$sum": "$L_QUANTITY"}}},
    {"$match": {"totalQ": {"$gt": 300}}}
])
lineitem_q1 = pd.DataFrame(list(query1))

# Extract rest needed data from MySQL
query2 = (
    "SELECT C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE "
    "FROM customer, orders "
    "WHERE O_ORDERKEY IN (" + ','.join(map(str, lineitem_q1['_id'])) + ") "
    "AND C_CUSTKEY = O_CUSTKEY"
)
mysql_cursor.execute(query2)
orders_q2 = pd.DataFrame(mysql_cursor.fetchall(), columns=['C_NAME','C_CUSTKEY','O_ORDERKEY','O_ORDERDATE','O_TOTALPRICE'])

# Merge Mongo lineitem and MySQL order data
final_df = pd.merge(orders_q2, lineitem_q1, left_on='O_ORDERKEY', right_on='_id', how='inner')
final_df = final_df.rename(columns={"totalQ": "SUM(L_QUANTITY)"}).drop(columns=['_id'])

# Write the DataFrame to a CSV
final_df.to_csv('query_output.csv', index=False)
