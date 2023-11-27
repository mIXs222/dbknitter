import csv
import pymongo
import mysql.connector
from pymongo import MongoClient
from operator import itemgetter

# Connecting to the MongoDB Database
def mongo_db():
  client = MongoClient('mongodb', 27017)
  db_mongo = client['tpch']
  return db_mongo

#Connecting to Mysql Database
def mysql_db():
  mydb = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
  )
  return mydb

def query_data():
  #Mongodb Data
  db_mongo = mongo_db()
  customer = list(db_mongo.customer.find({}, {'_id':0}))
  lineitem = list(db_mongo.lineitem.find({}, {'_id':0}))

  #Mysql data
  db_mysql = mysql_db()
  cursor = db_mysql.cursor()
  cursor.execute("SELECT * FROM orders")
  orders = cursor.fetchall()

  # Selecting necessary fields
  selected_data = []
  for cust in customer:
    for order in orders:
      for line in lineitem:
        if line['L_ORDERKEY'] == order[0] and cust['C_CUSTKEY'] == order[1] and \
        order[4] < '1995-03-15' and line['L_SHIPDATE'] > '1995-03-15' and cust['C_MKTSEGMENT'] == 'BUILDING':
          selected_data.append({'L_ORDERKEY': line['L_ORDERKEY'], 
                                'REVENUE': line['L_EXTENDEDPRICE'] * (1 - line['L_DISCOUNT']), 
                                'O_ORDERDATE': order[4], 
                                'O_SHIPPRIORITY': order[7]})
  
  # Group by and Order by
  selected_data.sort(key=itemgetter('L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'))
  selected_data.sort(key=itemgetter('REVENUE'), reverse=True)
  
  keys = selected_data[0].keys()
  with open('query_output.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(selected_data)

query_data()
