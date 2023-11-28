import pymongo
import csv

# Connect to the MongoDB server
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# Query the 'customer' collection and 'orders' collection
customers = list(db.customer.find({}, {'_id': 0, 'C_CUSTKEY': 1}))
orders = list(db.orders.find({'$and': [{'O_COMMENT': {'$not': {'$regex': 'pending|deposits'}}}]},
                             {'_id': 0, 'O_CUSTKEY': 1}))

# Convert orders to a dictionary where key is customer_key and value is count of orders
orders_dict = {}
for order in orders:
    key = order['O_CUSTKEY']
    if key in orders_dict:
        orders_dict[key] += 1
    else:
        orders_dict[key] = 1

# Prepare the list with customer_key and order counts
cust_order_count = [[cust['C_CUSTKEY'], orders_dict.get(cust['C_CUSTKEY'], 0)] for cust in customers]

# Create a dictionary where key is order_count and value is count of customers with that order_count
cust_dist = {}
for cust_count in cust_order_count:
    count = cust_count[1]
    if count in cust_dist:
        cust_dist[count] += 1
    else:
        cust_dist[count] = 1

# Convert cust_dist to a list of lists for the csv writer and sort it
results = [[count, cust_dist[count]] for count in cust_dist]
results.sort(key=lambda x: (-x[1], -x[0]))

# Write the results to the file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['C_COUNT', 'CUSTDIST']) # Column headers
    writer.writerows(results)
