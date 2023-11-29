from pymongo import MongoClient
import csv

# Connection to the MongoDB server
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Aggregation pipeline to execute the query
pipeline = [
    {'$match': {'O_ORDERSTATUS': {'$nin': ['pending', 'deposit']}}},
    {'$group': {'_id': '$O_CUSTKEY', 'num_orders': {'$sum': 1}}},
    {'$match': {'num_orders': {'$gt': 0}}},
    {'$group': {'_id': '$num_orders', 'num_customers': {'$sum': 1}}},
    {'$sort': {'_id': 1}},
    {'$project': {'_id': 0, 'num_orders': '$_id', 'num_customers': 1}},
]

# Execute the query and store the result
result = list(db.orders.aggregate(pipeline))

# Write output to the file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['num_orders', 'num_customers']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in result:
        writer.writerow({'num_orders': data['num_orders'], 'num_customers': data['num_customers']})

client.close()
