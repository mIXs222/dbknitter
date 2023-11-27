import csv
import pymongo
import mysql.connector
from collections import defaultdict

# Connecting to MySQL
mysql_conn = mysql.connector.connect(
    host="mysql",
    user="root",
    passwd="my-secret-pw",
    database="tpch"
)
mysql_cursor = mysql_conn.cursor()

# Getting data from orders table
mysql_cursor.execute("SELECT * FROM orders")
mysql_data = mysql_cursor.fetchall()

# Creating a dictionary from MySQL data
mysql_dict = defaultdict(list)
for row in mysql_data:
    mysql_dict[row[1]].append(row[0])

# Connecting to MongoDB
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]

# Getting data from customers collection
mongodb_data = mongodb_db["customer"].find()

# Executing the query
result = {}
for row in mongodb_data:
    custkey = row["C_CUSTKEY"]
    if custkey in mysql_dict:
        order_count = len(mysql_dict[custkey])
        if order_count not in result:
            result[order_count] = 0
        result[order_count] += 1

# Writing the result to CSV
with open("query_output.csv", "w") as output_file:
    writer = csv.writer(output_file)
    for key, value in sorted(result.items(), key=lambda item: (-item[1], -item[0])):
        writer.writerow([key, value])
