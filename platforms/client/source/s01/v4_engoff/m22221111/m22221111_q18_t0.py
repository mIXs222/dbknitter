from pymongo import MongoClient
import csv

# Connection details for the MongoDB instance.
hostname = 'mongodb'
port = 27017
database_name = 'tpch'

# Connect to MongoDB using pymongo.
client = MongoClient(host=hostname, port=port)
db = client[database_name]

# Aggregation pipeline to find customers with large quantity orders.
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'C_CUSTKEY',
            'foreignField': 'O_CUSTKEY',
            'as': 'orders'
        }
    },
    {'$unwind': '$orders'},
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'orders.O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    {'$unwind': '$lineitems'},
    {
        '$group': {
            '_id': {
                'C_CUSTKEY': '$C_CUSTKEY',
                'C_NAME': '$C_NAME',
                'O_ORDERKEY': '$orders.O_ORDERKEY',
                'O_ORDERDATE': '$orders.O_ORDERDATE',
                'O_TOTALPRICE': '$orders.O_TOTALPRICE'
            },
            'total_quantity': {'$sum': '$lineitems.L_QUANTITY'}
        }
    },
    {
        '$match': {
            'total_quantity': {'$gt': 300}
        }
    },
    {
        '$project': {
            '_id': 0,
            'C_NAME': '$_id.C_NAME',
            'C_CUSTKEY': '$_id.C_CUSTKEY',
            'O_ORDERKEY': '$_id.O_ORDERKEY',
            'O_ORDERDATE': '$_id.O_ORDERDATE',
            'O_TOTALPRICE': '$_id.O_TOTALPRICE',
            'total_quantity': 1
        }
    }
]

# Fetch and write the results.
query_results = db.customer.aggregate(pipeline)

# Write the results to 'query_output.csv'.
with open('query_output.csv', mode='w', newline='') as csv_file:
    fieldnames = ['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'total_quantity']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    
    for doc in query_results:
        writer.writerow(doc)

# Close the MongoDB connection.
client.close()
