# top_supplier.py
import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
supplier_collection = mongodb['supplier']

# Query to select all suppliers in MySQL
mysql_query = """
    SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
    FROM lineitem
    WHERE L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE < '1996-04-01'
    GROUP BY L_SUPPKEY
"""

top_revenue = 0
top_suppliers = []

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    for row in cursor:
        supplier_revenue = row[1]
        if supplier_revenue > top_revenue:
            top_revenue = supplier_revenue
            top_suppliers = [row[0]]
        elif supplier_revenue == top_revenue:
            top_suppliers.append(row[0])

# Query MongoDB for the top supplier details
top_suppliers_info = list(supplier_collection.find({'S_SUPPKEY': {'$in': top_suppliers}}, {'_id': 0}))

# Sorting suppliers
top_suppliers_sorted = sorted(top_suppliers_info, key=lambda x: x['S_SUPPKEY'])

# Write to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # Writing headers
    writer.writerow(top_suppliers_sorted[0].keys())
    for supplier in top_suppliers_sorted:
        writer.writerow(supplier.values())

# Close connections
mysql_conn.close()
mongo_client.close()
