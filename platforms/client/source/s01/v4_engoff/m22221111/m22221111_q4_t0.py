from pymongo import MongoClient
import csv

# Connect to the MongoDB server
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the date range for the query
date_start = "1993-07-01"
date_end = "1993-10-01"

# Query the database
orders = db.orders.find({"O_ORDERDATE": {"$gte": date_start, "$lt": date_end}})
orders_with_late_lineitem = {}

# Checking for orders with late lineitems
for order in orders:
    late_lineitems = db.lineitem.find({
        "L_ORDERKEY": order["O_ORDERKEY"],
        "L_COMMITDATE": {"$lt": "L_RECEIPTDATE"}
    })
    
    if late_lineitems.count() > 0:
        priority = order['O_ORDERPRIORITY']
        orders_with_late_lineitem[priority] = orders_with_late_lineitem.get(priority, 0) + 1

# Write the results to a CSV file
with open('query_output.csv', mode='w') as csv_file:
    fieldnames = ['O_ORDERPRIORITY', 'ORDER_COUNT']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for priority, count in sorted(orders_with_late_lineitem.items()):
        writer.writerow({'O_ORDERPRIORITY': priority, 'ORDER_COUNT': count})

# Close the MongoDB connection
client.close()
