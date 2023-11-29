# python_code.py

import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# MongoDB connection setup
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
supplier_table = mongodb_db['supplier']

# Function to convert supplier documents into a dictionary
def supplier_dict(supplier_docs):
    supplier_info = {}
    for doc in supplier_docs:
        supplier_info[doc["S_SUPPKEY"]] = {
            "S_NAME": doc["S_NAME"],
            "S_ADDRESS": doc["S_ADDRESS"],
            "S_PHONE": doc["S_PHONE"]
        }
    return supplier_info

# Fetch supplier info from MongoDB
suppliers = supplier_dict(supplier_table.find())

# Fetch lineitem info from MySQL
with mysql_conn.cursor() as cursor:
    query = """
    SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
    FROM lineitem
    WHERE L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE < '1996-04-01'
    GROUP BY L_SUPPKEY
    ORDER BY total_revenue DESC, L_SUPPKEY ASC
    """
    cursor.execute(query)
    lineitem_results = cursor.fetchall()

# Compute the maximum revenue and select suppliers with that revenue
max_revenue = max(lineitem_results, key=lambda x: x[1])[1] if lineitem_results else 0
top_suppliers = [row for row in lineitem_results if row[1] == max_revenue]

# Write to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
    for suppkey, revenue in top_suppliers:
        if suppkey in suppliers:
            writer.writerow([
                suppkey,
                suppliers[suppkey]['S_NAME'],
                suppliers[suppkey]['S_ADDRESS'],
                suppliers[suppkey]['S_PHONE'],
                round(revenue, 2)  # Assuming rounding to 2 decimal places
            ])

# Close connections
mysql_conn.close()
mongodb_client.close()
