from pymongo import MongoClient
import csv
import mysql.connector

# Connect to mongodb
client = MongoClient("mongodb://localhost:27017/")
mongodb = client['tpch']
orders = mongodb['orders']
lineitem = mongodb['lineitem']

# Connect to MySQL
mydb = mysql.connector.connect(
  host="localhost",
  user="yourUserName",
  password="yourPassword",
  database="tpch"
)

mycursor = mydb.cursor()

# Get order records within given dates
mycursor.execute("SELECT * FROM orders WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'")
mysql_orders = mycursor.fetchall()

# Perform steps in the subquery
subquery_ids = set()
for record in lineitem.find({}):
    if record['L_COMMITDATE'] < record['L_RECEIPTDATE']:
        subquery_ids.add(record['L_ORDERKEY'])

# Perform steps in the outer query
output = []
for order in mysql_orders:
    if order[0] in subquery_ids:
        output.append(order)

# Writing to query_output.csv
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
    for row in output:
        writer.writerow(row)
