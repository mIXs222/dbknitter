import csv
import mysql.connector
import pymongo

# Connect to MySQL
mysql_db = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

# Fetch MySQL data
mysql_cursor = mysql_db.cursor()
mysql_query = 'YOUR MYSQL QUERY GOES HERE'
mysql_cursor.execute(mysql_query)

mysql_results = mysql_cursor.fetchall()

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Fetch MongoDB data
mongodb_query = 'YOUR MONGODB QUERY GOES HERE'

mongodb_results = mongodb.command(mongodb_query)

# Combine the results and write to csv 
combined_results = mysql_results + mongodb_results

with open('query_output.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(combined_results)
