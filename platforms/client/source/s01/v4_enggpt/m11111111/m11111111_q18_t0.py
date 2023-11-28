from pymongo import MongoClient
import csv

# Connect to the MongoDB database
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Aggregation pipeline to get the total quantities per order key from the lineitem table
pipeline = [
    {
        '$group': {
            '_id': '$L_ORDERKEY',
            'total_quantity': {'$sum': '$L_QUANTITY'}
        }
    },
    {'$match': {'total_quantity': {'$gt': 300}}}
]

# Run the pipeline
order_keys_with_total_gt_300 = db.lineitem.aggregate(pipeline)

# Extract just the '_id' (which is the O_ORDERKEY)
order_keys_set = {item['_id'] for item in order_keys_with_total_gt_300}

# Query to find orders from these order keys
matched_orders = list(db.orders.find({'O_ORDERKEY': {'$in': list(order_keys_set)}}))

# Build a dict with order details
orders_dict = {
    order['O_ORDERKEY']: {
        'O_CUSTKEY': order['O_CUSTKEY'],
        'O_ORDERDATE': order['O_ORDERDATE'],
        'O_TOTALPRICE': order['O_TOTALPRICE']
    }
    for order in matched_orders
}

# Find corresponding customers
customer_keys = [order['O_CUSTKEY'] for order in matched_orders]
matched_customers = list(db.customer.find({'C_CUSTKEY': {'$in': customer_keys}}))

# Build a dict with customer details
customers_dict = {cust['C_CUSTKEY']: cust['C_NAME'] for cust in matched_customers}

# Build the results
results = []
for order_key, order in orders_dict.items():
    cust_name = customers_dict.get(order['O_CUSTKEY'])
    if cust_name:
        results.append({
            'C_NAME': cust_name,
            'C_CUSTKEY': order['O_CUSTKEY'],
            'O_ORDERKEY': order_key,
            'O_ORDERDATE': order['O_ORDERDATE'],
            'O_TOTALPRICE': order['O_TOTALPRICE'],
            'total_quantity': next((item['total_quantity'] for item in order_keys_set if item['_id'] == order_key), None)
        })

# Sort the results by total price descending and order date
results_sorted = sorted(results, key=lambda k: (-k['O_TOTALPRICE'], k['O_ORDERDATE']))

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=results_sorted[0].keys())
    writer.writeheader()
    writer.writerows(results_sorted)
