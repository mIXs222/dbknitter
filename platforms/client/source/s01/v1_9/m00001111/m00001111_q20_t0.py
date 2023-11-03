import csv
import mysql.connector
import pymongo
from pymongo import MongoClient

def mysql_connection():
    mydb = mysql.connector.connect(
        host="mysql",
        user="root",
        password="my-secret-pw",
        database="tpch"
    )
    mycursor = mydb.cursor()

    mycursor.execute("SELECT S_NAME, S_ADDRESS FROM supplier, nation WHERE S_NATIONKEY = N_NATIONKEY AND N_NAME = 'CANADA' ORDER BY S_NAME;")
    
    myresult = mycursor.fetchall()

    for x in myresult:
        print(x)

def mongo_connection():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['tpch']
    cust_coll = db['customer']
    cust_data = cust_coll.find()
    for data in cust_data:
        print(data)


mysql_connection()
mongo_connection()
