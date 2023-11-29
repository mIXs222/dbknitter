from pymongo import MongoClient
import csv

# MongoDB Connection
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Aggregation Pipeline to Perform the Query
pipeline = [
    # Join orders with lineitem on O_ORDERKEY == L_ORDERKEY
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    # Unwind the lineitems array to simulate a JOIN
    {'$unwind': '$lineitems'},
    # Group by to calculate total quantity per order
    {
        '$group': {
            '_id': {
                'O_ORDERKEY': '$O_ORDERKEY',
                'O_CUSTKEY': '$O_CUSTKEY',
                'O_TOTALPRICE': '$O_TOTALPRICE',
                'O_ORDERDATE': '$O_ORDERDATE'
            },
            'total_quantity': {'$sum': '$lineitems.L_QUANTITY'}
        }
    },
    # Having clause to filter orders with total quantity > 300
    {'$match': {'total_quantity': {'$gt': 300}}},
    # Join customers with our filtered orders on C_CUSTKEY == O_CUSTKEY
    {
        '$lookup': {
            'from': 'customer',
            'localField': '_id.O_CUSTKEY',
            'foreignField': 'C_CUSTKEY',
            'as': 'customers'
        }
    },
    {'$unwind': '$customers'},
    # Select the fields
    {
        '$project': {
            '_id': 0,
            'C_NAME': '$customers.C_NAME',
            'C_CUSTKEY': '$_id.O_CUSTKEY',
            'O_ORDERKEY': '$_id.O_ORDERKEY',
            'O_ORDERDATE': '$_id.O_ORDERDATE',
            'O_TOTALPRICE': '$_id.O_TOTALPRICE',
            'total_quantity': '$total_quantity'
        }
    },
    # Sort by O_TOTALPRICE descending and O_ORDERDATE ascending
    {'$sort': {'O_TOTALPRICE': -1, 'O_ORDERDATE': 1}}
]

# Execute the Query
results = db.orders.aggregate(pipeline)

# Write the results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    # Write CSV Header
    writer.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'total_quantity'])

    # Write Data
    for result in results:
        writer.writerow([result['C_NAME'], result['C_CUSTKEY'], result['O_ORDERKEY'], result['O_ORDERDATE'], result['O_TOTALPRICE'], result['total_quantity']])
