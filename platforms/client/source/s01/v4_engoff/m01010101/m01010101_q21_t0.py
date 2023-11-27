import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch

# Query MySQL for nation key of 'SAUDI ARABIA'
mysql_cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'SAUDI ARABIA';")
saudi_arabia_nation_key = mysql_cursor.fetchone()[0]

# Query MongoDB for suppliers from 'SAUDI ARABIA' and store their keys
suppliers_cursor = mongodb.supplier.find({'S_NATIONKEY': saudi_arabia_nation_key}, {'S_SUPPKEY': 1})
suppliers = [supplier['S_SUPPKEY'] for supplier in suppliers_cursor]

# Query the orders with status 'F' and their lineitems
mysql_cursor.execute("SELECT O_ORDERKEY FROM orders WHERE O_ORDERSTATUS = 'F';")
orders_with_status_f = [order[0] for order in mysql_cursor.fetchall()]

# Find suppliers who kept orders waiting
suppliers_who_kept_waiting = []
for order_key in orders_with_status_f:
    lineitems_cursor = mongodb.lineitem.find(
        {'L_ORDERKEY': order_key, 'L_COMMITDATE': {'$lt': 'L_RECEIPTDATE'}},
        {'L_SUPPKEY': 1}
    )
    lineitem_suppliers_set = {lineitem['L_SUPPKEY'] for lineitem in lineitems_cursor}
    # Check if there's only one supplier from SAUDI ARABIA who failed
    if len(lineitem_suppliers_set) > 1 and lineitem_suppliers_set.issubset(set(suppliers)):
        suppliers_who_kept_waiting.extend(lineitem_suppliers_set)

# Remove duplicates and fetch supplier details
unique_suppliers_who_kept_waiting = list(set(suppliers_who_kept_waiting))
suppliers_details = mongodb.supplier.find(
    {'S_SUPPKEY': {'$in': unique_suppliers_who_kept_waiting}},
    {'S_NAME': 1, 'S_ADDRESS': 1, 'S_PHONE': 1, '_id': 0}
)

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_NAME', 'S_ADDRESS', 'S_PHONE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for supplier in suppliers_details:
        writer.writerow(supplier)

# Closing the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
