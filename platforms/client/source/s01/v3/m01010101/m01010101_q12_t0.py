import mysql.connector
from pymongo import MongoClient
import pandas as pd

# Database Connection

# MySQL
mysql_db = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
mysql_cursor = mysql_db.cursor()

# MongoDB
mongo_client = MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Pull data

# MySQL data
mysql_cursor.execute("SELECT * FROM orders")
mysql_data = mysql_cursor.fetchall()
mysql_df = pd.DataFrame(mysql_data, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

# MongoDB data
mongo_data = list(mongo_db.lineitem.find())
mongo_df = pd.DataFrame(mongo_data)

# Merge MySQL and MongoDB data
merged_df = pd.merge(mysql_df, mongo_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Query manipulation

# Apply the conditions
filtered_df = merged_df[(merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) & (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) & (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) & (merged_df['L_RECEIPTDATE'] >= '1994-01-01') & (merged_df['L_RECEIPTDATE'] < '1995-01-01')]

# Add new columns
filtered_df['HIGH_LINE_COUNT'] = filtered_df['O_ORDERPRIORITY'].apply(lambda x: 1 if x in ['1-URGENT','2-HIGH'] else 0)
filtered_df['LOW_LINE_COUNT'] = filtered_df['O_ORDERPRIORITY'].apply(lambda x: 0 if x in ['1-URGENT','2-HIGH'] else 1)

# Group by
grouped_df = filtered_df.groupby('L_SHIPMODE').sum()

# Order by
final_df = grouped_df[['HIGH_LINE_COUNT', 'LOW_LINE_COUNT']].sort_values(by='L_SHIPMODE')

# Output to CSV
final_df.to_csv('query_output.csv')
