import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']

# Get the data from MySQL
mysql_cursor = mysql_connection.cursor()
query = """
SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
FROM lineitem
WHERE L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE < '1996-04-01'
GROUP BY L_SUPPKEY
ORDER BY total_revenue DESC, L_SUPPKEY
"""
mysql_cursor.execute(query)
suppliers_revenue_results = mysql_cursor.fetchall()

# If no results, exit the program
if not suppliers_revenue_results:
    print("No supplier data found for the specified period.")
    exit()

# Get the maximum revenue
max_revenue = suppliers_revenue_results[0][1]

# Filter out the top suppliers
top_suppliers = [row[0] for row in suppliers_revenue_results if row[1] == max_revenue]

# Fetch supplier details from MongoDB for the top suppliers
top_suppliers_details = list(supplier_collection.find({"S_SUPPKEY": {"$in": top_suppliers}}))

# Write results to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT", "TOTAL_REVENUE"])
    
    for supplier in top_suppliers_details:
        writer.writerow([
            supplier["S_SUPPKEY"],
            supplier["S_NAME"],
            supplier["S_ADDRESS"],
            supplier["S_NATIONKEY"],
            supplier["S_PHONE"],
            supplier["S_ACCTBAL"],
            supplier["S_COMMENT"],
            next((supplier_revenue[1] for supplier_revenue in suppliers_revenue_results if supplier_revenue[0] == supplier["S_SUPPKEY"]), None)
        ])

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
