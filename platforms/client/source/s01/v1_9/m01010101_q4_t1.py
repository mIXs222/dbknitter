import pymysql
import pandas as pd
import pymongo
from pymongo import MongoClient
from pandas.io.json import json_normalize

# Define MySQL connection
mysql_con = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch')

# Define MongoDB connection
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Query on MySQL - ORDERS table
orders_query = """
SELECT O_ORDERPRIORITY, O_ORDERKEY
FROM orders
WHERE O_ORDERDATE >= '1993-07-01'
AND O_ORDERDATE < '1993-10-01'
GROUP BY O_ORDERKEY, O_ORDERPRIORITY; """

orders_df = pd.read_sql(orders_query, mysql_con)

# Query on MongoDB - lineitem table
lineitem_data = db.lineitem.find({ "L_COMMITDATE": {"$lt" : "L_RECEIPTDATE"}})

# Load into DataFrame
lineitem_df = json_normalize(lineitem_data)

# Join conditions
final_df = pd.merge(orders_df, lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group by and order operation
result_df = final_df.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT').sort_values(by='O_ORDERPRIORITY')

# Save results to csv file
result_df.to_csv('query_output.csv', index=False)
