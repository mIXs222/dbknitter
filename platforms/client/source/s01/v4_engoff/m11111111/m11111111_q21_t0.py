from pymongo import MongoClient
import csv

# Establish a connection to the MongoDB server
client = MongoClient('mongodb', 27017)

# Select the database and collections
db = client['tpch']
nation_collection = db['nation']
supplier_collection = db['supplier']
orders_collection = db['orders']
lineitem_collection = db['lineitem']

# Find the nation key for Saudi Arabia
nation_key = None
for nation in nation_collection.find({'N_NAME': 'SAUDI ARABIA'}):
    nation_key = nation['N_NATIONKEY']
    break

# Ensure the nation exists before proceeding
if nation_key is None:
    print("Nation 'SAUDI ARABIA' not found.")
    exit()

# Prepare to write output to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write headers to CSV
    writer.writerow(['SUPPLIER NO', 'SUPPLIER NAME'])

    # Find suppliers from Saudi Arabia
    supplier_keys = [supplier['S_SUPPKEY'] for supplier in supplier_collection.find({'S_NATIONKEY': nation_key})]

    # Find orders with status 'F'
    orders_with_status_f = orders_collection.find({'O_ORDERSTATUS': 'F'})

    # Iterate over the orders and get details
    for order in orders_with_status_f:
        order_key = order['O_ORDERKEY']
        
        # Find lineitems with supplier key in our supplier list, and the conditions mentioned
        lineitem_cursor = lineitem_collection.aggregate([
            {
                '$match': {
                    'L_ORDERKEY': order_key,
                    'L_SUPPKEY': {'$in': supplier_keys},
                    'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'}
                }
            },
            {
                '$group': {
                    '_id': '$L_ORDERKEY',
                    'suppliers': {'$addToSet': '$L_SUPPKEY'},
                    'count': {'$sum': 1}
                }
            }
        ])

        # Iterate over the result cursor
        for lineitem in lineitem_cursor:
            if lineitem['count'] == 1:  # Only one supplier responsible for the delay
                # Fetch the supplier's details and write to CSV
                supplier_key = lineitem['suppliers'][0]
                supplier_details = supplier_collection.find_one({'S_SUPPKEY': supplier_key})
                writer.writerow([supplier_key, supplier_details['S_NAME']])

client.close()
