import pymysql
from pymongo import MongoClient
import pandas as pd

# Connect to MySQL
mydb = pymysql.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mycursor = mydb.cursor()

# Select orders from MySQL
mycursor.execute("SELECT O_ORDERKEY, O_ORDERPRIORITY FROM orders WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'")
orders = mycursor.fetchall()

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch

# Fields for dataframe
fields = ['O_ORDERPRIORITY', 'ORDER_COUNT']
df = pd.DataFrame(columns=fields)

# Loop through each order
for order in orders:
    # Find matching entries in lineitem
    lineitems = db.lineitem.find({'L_ORDERKEY': order[0], 'L_COMMITDATE': {'$lt': 'L_RECEIPTDATE'}})
    if lineitems.count() > 0:
        df = df.append({'O_ORDERPRIORITY': order[1], 'ORDER_COUNT': lineitems.count()}, ignore_index=True)

# Group by O_ORDERPRIORITY
df = df.groupby(['O_ORDERPRIORITY']).size().reset_index(name='ORDER_COUNT')
df.sort_values(by=['O_ORDERPRIORITY'], inplace=True)

# Write to csv
df.to_csv('query_output.csv', index=False)
