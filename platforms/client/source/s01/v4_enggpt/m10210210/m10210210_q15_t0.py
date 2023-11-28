# Import required libraries
import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
supplier_collection = mongodb['supplier']

# Get supplier details from MongoDB
suppliers = list(supplier_collection.find({}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1, 'S_PHONE': 1}))

# Dates for the quarter
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)

# Query lineitem from MySQL to get total revenue per supplier
revenue_query = """
    SELECT L_SUPPKEY AS SUPPLIER_NO, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
    FROM lineitem
    WHERE L_SHIPDATE >= %s AND L_SHIPDATE <= %s
    GROUP BY L_SUPPKEY
"""
mysql_cursor.execute(revenue_query, (start_date, end_date))

# Store revenue data in a dictionary
revenue_data = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Merge supplier details with revenue and identify max revenue supplier
max_revenue = 0
max_revenue_supplier = None

for supplier in suppliers:
    supplier_no = supplier['S_SUPPKEY']
    supplier['TOTAL_REVENUE'] = revenue_data.get(supplier_no, 0)
    if supplier['TOTAL_REVENUE'] > max_revenue:
        max_revenue = supplier['TOTAL_REVENUE']
        max_revenue_supplier = supplier

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    if max_revenue_supplier:
        writer.writerow(max_revenue_supplier)
