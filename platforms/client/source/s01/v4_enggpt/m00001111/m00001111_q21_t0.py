# Python Code to execute the query (`query.py`)

import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to the MySQL database
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']

# Cursors for executing MySQL queries
mysql_cursor = mysql_connection.cursor()

# Query to get suppliers from Saudi Arabia
mysql_cursor.execute("""
SELECT S_NAME, S_SUPPKEY
FROM supplier
JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
WHERE N_NAME = 'SAUDI ARABIA'
""")

# Mapping of supplier names to supplier keys for those located in SA
supplier_data = {supp_key: supp_name for supp_name, supp_key in mysql_cursor.fetchall()}

# Now, query MongoDB for orders with status 'F' and related line item details
matching_lineitems = mongodb['lineitem'].aggregate([
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'order_info'
        }
    },
    {'$unwind': '$order_info'},
    {'$match': {'order_info.O_ORDERSTATUS': 'F', 'L_RECEIPTDATE': {'$gt': '$L_COMMITDATE'}}},
    {
        '$project': {
            'L_SUPPKEY': 1,
            'L_ORDERKEY': 1,
            'L_RECEIPTDATE': 1,
            'L_COMMITDATE': 1,
            'order_info.O_ORDERSTATUS': 1
        }
    }
])

# Calculate waiting times
waiting_times = {}
for item in matching_lineitems:
    supp_key = item['L_SUPPKEY']
    if supp_key in supplier_data:
        waiting_times.setdefault(supp_key, 0)
        if item['L_RECEIPTDATE'] > item['L_COMMITDATE']:
            waiting_times[supp_key] += 1  # Increase count if criteria meet

# Write output to CSV
with open('query_output.csv', 'w', newline='') as csv_file:
    fieldnames = ['S_NAME', 'NUMWAIT']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    for supp_key, num_wait in sorted(waiting_times.items(), key=lambda x: (-x[1], supplier_data[x[0]])):
        writer.writerow({'S_NAME': supplier_data[supp_key], 'NUMWAIT': num_wait})

# Close the connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
