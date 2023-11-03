import pymysql.cursors
import pymongo
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]

# Execute the query in MySQL
with connection.cursor() as cursor:
    sql = "YOUR QUERY HERE"
    cursor.execute(sql)
    result_mysql = cursor.fetchall()

# Convert the result to a list of dicts
result_mysql_list = list(result_mysql)

# Execute the query in MongoDB
result_mongodb = mongodb.region.aggregate([
    {"$lookup": {
        "from": "supplier",
        "localField": "S_NATIONKEY",
        "foreignField": "S_SUPPKEY",
        "as": "supplier_region"
    }},
    {"$lookup": {
        "from": "partsupp",
        "localField": "PS_PARTKEY",
        "foreignField": "PS_SUPPKEY",
        "as": "partsupp_region"
    }},
    # Continue for all queries in your original query
])

# Convert the result to a list of dicts
result_mongodb_list = list(result_mongodb)

# Merge results
result_merged = result_mysql_list + result_mongodb_list

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = result_merged[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in result_merged:
        writer.writerow(row)
