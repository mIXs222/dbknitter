# query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# Function to get revenue from MySQL
def get_revenue(mysql_conn):
    cur = mysql_conn.cursor()
    cur.execute("""
    SELECT
        L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
    FROM
        lineitem
    WHERE
        L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE < '1996-04-01'
    GROUP BY
        L_SUPPKEY
    """)
    revenue_data = cur.fetchall()
    cur.close()
    return revenue_data

# Function to get supplier details from MongoDB
def get_supplier_details(mongo_conn, supplier_keys):
    supplier_details = {}
    for supp_key in supplier_keys:
        supplier_detail = mongo_conn.tpch.supplier.find_one({'S_SUPPKEY': supp_key})
        supplier_details[supp_key] = supplier_detail
    return supplier_details

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_conn = mongo_client.tpch

# Get revenues and supplier keys from MySQL
revenues = get_revenue(mysql_conn)

# Find the maximum revenue
max_revenue = max([rev['TOTAL_REVENUE'] for rev in revenues])

# Get the supplier keys with the maximum revenue
max_supplier_keys = [rev['L_SUPPKEY'] for rev in revenues if rev['TOTAL_REVENUE'] == max_revenue]

# Get supplier details from MongoDB
suppliers_info = get_supplier_details(mongo_conn, max_supplier_keys)

# Write the result to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
    # Write each supplier's detail and revenue
    for supp_key in sorted(max_supplier_keys):
        supplier = suppliers_info[supp_key]
        if supplier:
            writer.writerow([
                supplier['S_SUPPKEY'],
                supplier['S_NAME'],
                supplier['S_ADDRESS'],
                supplier['S_PHONE'],
                max_revenue
            ])

# Close the connections
mysql_conn.close()
mongo_client.close()
