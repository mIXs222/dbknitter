from pymongo import MongoClient
import mysql.connector
import csv
from collections import defaultdict

# Connect to MySQL database
mysql_db = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

# Connect to MongoDB database
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get data from MongoDB
customer_data = list(mongo_db['customer'].find({}, {"_id": 0}))
lineitem_data = list(mongo_db['lineitem'].find({}, {"_id": 0}))

# Get data from MySQL
mysql_cursor = mysql_db.cursor()
mysql_cursor.execute("SELECT * FROM ORDERS")
mysql_data = mysql_cursor.fetchall()
mysql_db.close()

# Convert MySQL data to dictionary
orders_dict = defaultdict(list)
for row in mysql_data:
    orders_dict[row[1]].append(row[0])

# Process data from MongoDB
result = defaultdict(int)
for row in customer_data:
    count = len([i for i in orders_dict[row['C_CUSTKEY']] if 'pending' not in i and 'deposits' not in i])
    if count:
        result[count] += 1

# Write output to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["C_COUNT", "CUSTDIST"])
    for key, value in result.items():
        writer.writerow([key, value])
