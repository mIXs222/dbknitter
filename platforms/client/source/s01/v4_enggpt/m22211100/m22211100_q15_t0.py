import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_supplier = mongo_db['supplier']

# Define the date range for lineitem shipment
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)

# Perform the MySQL query for revenue calculation
mysql_query = """
    SELECT L_SUPPKEY as SUPPLIER_NO, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
    FROM lineitem
    WHERE L_SHIPDATE BETWEEN %s AND %s
    GROUP BY L_SUPPKEY
"""
mysql_cursor.execute(mysql_query, (start_date, end_date))

# Fetching the revenue data from MySQL
revenue_data = {}
for row in mysql_cursor:
    revenue_data[row[0]] = row[1]

# Fetch the supplier data from MongoDB
supplier_data = list(mongo_supplier.find({}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1, 'S_PHONE': 1}))

# Combining the data
combined_data = []
for supplier in supplier_data:
    supp_key = supplier['S_SUPPKEY']
    if supp_key in revenue_data:
        combined_data.append({
            'S_SUPPKEY': supp_key,
            'S_NAME': supplier['S_NAME'],
            'S_ADDRESS': supplier['S_ADDRESS'],
            'S_PHONE': supplier['S_PHONE'],
            'TOTAL_REVENUE': revenue_data[supp_key]
        })

# Find the supplier with the maximum revenue
max_revenue_supplier = max(combined_data, key=lambda x: x['TOTAL_REVENUE'])

# Write the combined data to CSV
output_file = 'query_output.csv'
with open(output_file, mode='w', newline='') as file:
    csv_writer = csv.DictWriter(file, fieldnames=max_revenue_supplier.keys())
    csv_writer.writeheader()
    csv_writer.writerow(max_revenue_supplier)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
