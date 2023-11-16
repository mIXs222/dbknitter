import pymongo
import csv

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client['tpch']

# Retrieve all necessary data from MongoDB collections
suppliers = list(db.supplier.find({}, {'_id': 0}))
lineitems = list(db.lineitem.find({}, {'_id': 0}))
orders = list(db.orders.find({'O_ORDERSTATUS': 'F'}, {'_id': 0}))
nations = list(db.nation.find({'N_NAME': 'SAUDI ARABIA'}, {'_id': 0}))

# Filter lineitems where L_RECEIPTDATE > L_COMMITDATE
lineitems_filtered = [l for l in lineitems if l['L_RECEIPTDATE'] > l['L_COMMITDATE']]

# Create a mapping of orders that have lineitems
orders_with_lineitems = {}
for l in lineitems:
    if l['L_ORDERKEY'] not in orders_with_lineitems:
        orders_with_lineitems[l['L_ORDERKEY']] = set()
    orders_with_lineitems[l['L_ORDERKEY']].add(l['L_SUPPKEY'])

# Filter out the suppliers
suppliers_filtered = [s for s in suppliers if any(n['N_NATIONKEY'] == s['S_NATIONKEY'] for n in nations)]

# Define the resulting list for supplier names and their counts
result_list = []

# Iterate through the suppliers to count the number of waiting lineitems
for supplier in suppliers_filtered:
    numwait = 0
    for order in orders:
        order_key = order['O_ORDERKEY']
        if order_key in orders_with_lineitems and supplier['S_SUPPKEY'] in orders_with_lineitems[order_key]:
            other_suppliers = orders_with_lineitems[order_key] - {supplier['S_SUPPKEY']}
            has_other_lineitems = any(
                l['L_ORDERKEY'] == order_key and 
                l['L_SUPPKEY'] in other_suppliers for l in lineitems_filtered
            )
            if not has_other_lineitems:
                numwait += 1
    if numwait > 0:
        result_list.append((supplier['S_NAME'], numwait))

# Sorting the result_list by NUMWAIT descending, then S_NAME
result_list.sort(key=lambda s: (-s[1], s[0]))

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['S_NAME', 'NUMWAIT'])
    for row in result_list:
        writer.writerow(row)
