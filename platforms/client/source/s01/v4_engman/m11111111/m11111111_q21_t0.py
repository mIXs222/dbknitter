import pymongo
import csv

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
database = client['tpch']

# Get collections
nations_collection = database['nation']
suppliers_collection = database['supplier']
orders_collection = database['orders']
lineitem_collection = database['lineitem']

# Query nations to get nation key for 'SAUDI ARABIA'
nation_key = None
for nation in nations_collection.find({'N_NAME': 'SAUDI ARABIA'}):
    nation_key = nation['N_NATIONKEY']
    break

# Proceed only if nation key for 'SAUDI ARABIA' found
if nation_key is not None:
    # Data structure to store results
    supplier_wait_counts = {}

    # Find suppliers in 'SAUDI ARABIA'
    supplier_keys = [s['S_SUPPKEY'] for s in suppliers_collection.find({'S_NATIONKEY': nation_key})]

    # Find multi-supplier orders that have lineitems with multiple different S_SUPPKEY
    orders_with_multiple_suppliers = orders_collection.aggregate([
        {
            '$lookup': {
                'from': 'lineitem',
                'localField': 'O_ORDERKEY',
                'foreignField': 'L_ORDERKEY',
                'as': 'lineitems'
            }
        },
        {
            '$match': {
                'lineitems.1': {'$exists': True},  # to ensure multiple suppliers
                'O_ORDERSTATUS': 'F'
            }
        }
    ])

    # Iterate over orders and lineitems to find the required conditions
    for order in orders_with_multiple_suppliers:
        order_suppliers = {}
        for lineitem in order['lineitems']:
            if lineitem['L_SUPPKEY'] in supplier_keys:
                order_suppliers[lineitem['L_SUPPKEY']] = (
                    order_suppliers.get(lineitem['L_SUPPKEY'], 0) +
                    (lineitem['L_COMMITDATE'] < lineitem['L_RECEIPTDATE'])
                )
        # Add to the count only if this supplier was the only one who was late
        for suppkey, late_count in order_suppliers.items():
            if late_count and len(order_suppliers) == 1:
                supplier_wait_counts[suppkey] = supplier_wait_counts.get(suppkey, 0) + 1

    # Sorting the results
    sorted_suppliers = sorted(
        ((suppliers_collection.find_one({'S_SUPPKEY': supp}), count) for supp, count in supplier_wait_counts.items()),
        key=lambda x: (-x[1], x[0]['S_NAME'])
    )

    # Write results to a CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['NUMWAIT', 'S_NAME']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for supplier, count in sorted_suppliers:
            writer.writerow({'NUMWAIT': count, 'S_NAME': supplier['S_NAME']})

# Close the client connection
client.close()
