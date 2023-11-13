import mysql.connector
import pymongo
import csv
import pandas as pd
from pymongo import MongoClient

# Connect to mysql database
mysql_db = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

cursor = mysql_db.cursor()
query = "SELECT * FROM orders"
cursor.execute(query)
orders = cursor.fetchall()
orders_df = pd.DataFrame(orders, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch
lineitem_collection = db.lineitem
lineitem_data = lineitem_collection.find()
lineitem_df = pd.DataFrame(list(lineitem_data))

# Perform merge and generate desired dataframe
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

merged_df['HIGH_LINE_COUNT'] = merged_df['O_ORDERPRIORITY'].apply(lambda x: 1 if x == '1-URGENT' or x == '2-HIGH' else 0)
merged_df['LOW_LINE_COUNT'] = merged_df['O_ORDERPRIORITY'].apply(lambda x: 1 if x != '1-URGENT' and x != '2-HIGH' else 0)

final_df = merged_df[(merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) & (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) 
                  & (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) 
                  & (merged_df['L_RECEIPTDATE'] >= '1994-01-01') 
                  & (merged_df['L_RECEIPTDATE'] < '1995-01-01')].groupby('L_SHIPMODE')['HIGH_LINE_COUNT', 'LOW_LINE_COUNT'].sum()

# Write to CSV
final_df.to_csv('query_output.csv')
