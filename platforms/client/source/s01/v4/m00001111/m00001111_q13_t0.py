# mongodb_query.py

from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Retrieve data from MongoDB
customers = list(db.customer.find({}, {'_id': 0}))
orders = list(db.orders.find({'O_COMMENT': {'$not': /.*pending.*deposits.*/}}, {'_id': 0}))

# Perform LEFT OUTER JOIN in Python
cust_orders = {}
for cust in customers:
    cust_orders[cust['C_CUSTKEY']] = {
        'C_CUSTKEY': cust['C_CUSTKEY'],
        'C_COUNT': 0
    }

for order in orders:
    if order['O_CUSTKEY'] in cust_orders:
        cust_orders[order['O_CUSTKEY']]['C_COUNT'] += 1

# Perform GROUP BY and COUNT in Python
cust_count_dict = {}
for k, v in cust_orders.items():
    c_count = v['C_COUNT']
    if c_count in cust_count_dict:
        cust_count_dict[c_count] += 1
    else:
        cust_count_dict[c_count] = 1

# Sort by C_COUNT in descending order
sorted_cust_count = sorted(cust_count_dict.items(), key=lambda x: (-x[1], -x[0]))

# Write output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['C_COUNT', 'CUSTDIST']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for C_COUNT, CUSTDIST in sorted_cust_count:
        writer.writerow({'C_COUNT': C_COUNT, 'CUSTDIST': CUSTDIST})

print("CSV file generated successfully.")
