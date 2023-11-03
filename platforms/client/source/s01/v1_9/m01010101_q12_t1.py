import pandas as pd
from pymongo import MongoClient
import mysql.connector

# Create connection to MongoDB
client = MongoClient("mongodb://localhost:27017/")
mongodb = client["tpch"]

# Create connection to MySQL
db = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# Declare cursor
cursor = db.cursor()

# Execute query on MySQL
cursor.execute("SELECT * FROM ORDERS")
orders = cursor.fetchall()

# Convert to DataFrame
orders_df = pd.DataFrame(orders, columns = ['O_ORDERKEY','O_CUSTKEY','O_ORDERSTATUS','O_TOTALPRICE','O_ORDERDATE','O_ORDERPRIORITY','O_CLERK','O_SHIPPRIORITY', 'O_COMMENT'])

# Get data from mongodb
lineitem = mongodb['lineitem']
lineitem_data = lineitem.find()

# Convert to DataFrame
lineitem_df = pd.DataFrame(list(lineitem_data))

# Merge ORDERS and lineitem data
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter DataFrame
filtered_df = merged_df[(merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) & 
                        (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) &
                        (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) &
                        (merged_df['L_RECEIPTDATE'] >= '1994-01-01') &
                        (merged_df['L_RECEIPTDATE'] < '1995-01-01')]

# Create new columns
filtered_df['HIGH_LINE_COUNT'] = filtered_df.apply(lambda x: 1 if x['O_ORDERPRIORITY'] in ['1-URGENT', '2-HIGH'] else 0, axis=1)
filtered_df['LOW_LINE_COUNT'] = filtered_df.apply(lambda x: 1 if x['O_ORDERPRIORITY'] not in ['1-URGENT', '2-HIGH'] else 0, axis=1)

# Group by L_SHIPMODE
grouped_df = filtered_df.groupby('L_SHIPMODE').agg({'HIGH_LINE_COUNT':'sum', 'LOW_LINE_COUNT':'sum'}).reset_index()

# Sort DataFrame
sorted_df = grouped_df.sort_values('L_SHIPMODE')

# Write DataFrame to CSV
sorted_df.to_csv('query_output.csv', index=False)
