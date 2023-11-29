# file: execute_query.py
import pymysql
import pymongo
import csv

# Define connection parameters for MySQL
mysql_conn_params = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch"
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_params)
mysql_cursor = mysql_conn.cursor()

# Define the query to execute in MySQL
mysql_query = """
    SELECT
        P_BRAND, P_TYPE, P_SIZE, PS_PARTKEY
    FROM
        part, partsupp
    WHERE
        part.P_PARTKEY = partsupp.PS_PARTKEY
        AND P_BRAND <> 'Brand#45'
        AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
        AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
"""
# Execute the MySQL query
mysql_cursor.execute(mysql_query)
parts_suppliers = mysql_cursor.fetchall()
mysql_conn.close()

# Extract part keys that satisfy the conditions
part_keys = {row[3] for row in parts_suppliers}

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']

# Use aggregation to filter out suppliers with complaints
pipeline = [
    {"$match": {"S_COMMENT": {"$not": {"$regex": ".*Customer.*Complaints.*"}}}},
    {"$project": {"S_SUPPKEY": 1}}
]
suppliers_no_complaints = supplier_collection.aggregate(pipeline)
supplier_keys_no_complaints = {doc['S_SUPPKEY'] for doc in suppliers_no_complaints}

# Combine the results from MySQL and MongoDB to get the final output
combined_result = [
    (row[0], row[1], row[2])
    for row in parts_suppliers
    if row[3] in supplier_keys_no_complaints
]

# Use Counter to count occurrences
from collections import Counter
counts = Counter(combined_result)

# Sort the results
sorted_results = sorted(counts.items(), key=lambda k: (-k[1], k[0][0], k[0][1], k[0][2]))

# Write the results to a CSV file
with open("query_output.csv", "w", newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(["P_BRAND", "P_TYPE", "P_SIZE", "COUNT"])
    for item in sorted_results:
        csv_writer.writerow(item[0] + (item[1],))

print("The query output has been written to query_output.csv")
