import mysql.connector
import pymongo
import pandas as pd
from sqlalchemy import create_engine

# Connect to MySQL
mysql_conn = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb:27017")
mongo_db = mongo_client["tpch"]

# Query orders from MySQL
mysql_cursor.execute("""
SELECT
    O_ORDERPRIORITY,
    O_ORDERKEY
FROM
    orders
WHERE
    O_ORDERDATE >= '1993-07-01'
    AND O_ORDERDATE < '1993-10-01'
"""
)
orders_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['O_ORDERPRIORITY', 'O_ORDERKEY'])

# Query lineitems from MongoDB
lineitems_df = pd.DataFrame(mongo_db.lineitem.find({}, {"L_ORDERKEY": 1, "L_COMMITDATE": 1, "L_RECEIPTDATE": 1}))
lineitems_df = lineitems_df[lineitems_df['L_COMMITDATE'] < lineitems_df['L_RECEIPTDATE']]

# Combine and process data
output_df = pd.merge(orders_df, lineitems_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
output_df = output_df[['O_ORDERPRIORITY']].groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

# Save the output
output_df.to_csv('query_output.csv', index=False)
