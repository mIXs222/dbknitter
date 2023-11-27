import csv
import pymongo
import mysql.connector
from datetime import datetime, timedelta
from pymongo import MongoClient

# Connect to the MongoDB server and the required database
client = MongoClient("mongodb://localhost:27017/")
db = client["tpch"]

# Get the MongoDB tables
lineitem = db["lineitem"]
supplier = db["supplier"]

# Retrieve and process the required data from MongoDB 
result = lineitem.aggregate([
    {
        "$match": {
            "L_SHIPDATE": {
                "$gte": datetime(1996, 1, 1),
                "$lt": datetime(1996, 4, 1)
            }
        }
    }, 
    {
        "$group": {
            "_id": "$L_SUPPKEY",
            "TOTAL_REVENUE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]} }
        }
    }
])

mongodb_res = list(result)

# Get the MySQL database
cnx = mysql.connector.connect(user='root', password='password',
                              host='localhost',
                              database='tpch')

cursor = cnx.cursor()

# Formulate and run MySQL queries
query = ("SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE "
         "FROM supplier WHERE S_SUPPKEY = %s")

for res in mongodb_res:
    cursor.execute(query, (res["_id"], ))
    mysql_res = cursor.fetchall()

    res["S_SUPPKEY"] = mysql_res[0][0]
    res["S_NAME"] = mysql_res[0][1]
    res["S_ADDRESS"] = mysql_res[0][2]
    res["S_PHONE"] = mysql_res[0][3]

# Write the results to the CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "TOTAL_REVENUE"])
    for r in mongodb_res:
        writer.writerow([r["S_SUPPKEY"], r["S_NAME"], r["S_ADDRESS"], r["S_PHONE"], r["TOTAL_REVENUE"]])
