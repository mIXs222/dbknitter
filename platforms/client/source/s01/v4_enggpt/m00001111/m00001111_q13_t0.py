from pymongo import MongoClient
import csv

# MongoDB Connection Setup
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_customers = mongo_db["customer"]
mongo_orders = mongo_db["orders"]

# Fetch customers from MongoDB
customers = list(mongo_customers.find({}, {"_id": 0, "C_CUSTKEY": 1}))

# Prepare dictionary to count orders for each customer
order_counts = {customer['C_CUSTKEY']: 0 for customer in customers}

# Fetch orders from MongoDB and filter out specific comments
for order in mongo_orders.find({"O_COMMENT": {"$not": {"$regex": "pending|deposits"}}}):
    cust_key = order["O_CUSTKEY"]
    if cust_key in order_counts:
        order_counts[cust_key] += 1

# Calculate distribution
distribution = {}
for count in order_counts.values():
    distribution[count] = distribution.get(count, 0) + 1

# Sort by CUSTDIST descending, then by C_COUNT descending
sorted_distribution = sorted(distribution.items(), key=lambda x: (-x[1], -x[0]))

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['C_COUNT', 'CUSTDIST']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for count, custdist in sorted_distribution:
        writer.writerow({'C_COUNT': count, 'CUSTDIST': custdist})
