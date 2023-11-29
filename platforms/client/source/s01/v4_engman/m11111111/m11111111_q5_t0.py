import pymongo
import csv
import os

# Connect to the MongoDB database
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# Aggregation pipeline for MongoDB
pipeline = [
    {'$match': {'O_ORDERDATE': {'$gte': '1990-01-01', '$lt': '1995-01-01'}}},
    {'$lookup': {
        'from': 'customer',
        'localField': 'O_CUSTKEY',
        'foreignField': 'C_CUSTKEY',
        'as': 'customer'
    }},
    {'$unwind': '$customer'},
    {'$lookup': {
        'from': 'nation',
        'localField': 'customer.C_NATIONKEY',
        'foreignField': 'N_NATIONKEY',
        'as': 'nation'
    }},
    {'$unwind': '$nation'},
    {'$lookup': {
        'from': 'region',
        'localField': 'nation.N_REGIONKEY',
        'foreignField': 'R_REGIONKEY',
        'as': 'region'
    }},
    {'$unwind': '$region'},
    {'$match': {'region.R_NAME': 'ASIA'}},
    {'$lookup': {
        'from': 'lineitem',
        'localField': 'O_ORDERKEY',
        'foreignField': 'L_ORDERKEY',
        'as': 'lineitem'
    }},
    {'$unwind': '$lineitem'},
    {'$lookup': {
        'from': 'supplier',
        'localField': 'lineitem.L_SUPPKEY',
        'foreignField': 'S_SUPPKEY',
        'as': 'supplier'
    }},
    {'$unwind': '$supplier'},
    {'$lookup': {
        'from': 'nation',
        'localField': 'supplier.S_NATIONKEY',
        'foreignField': 'N_NATIONKEY',
        'as': 'supplier_nation'
    }},
    {'$unwind': '$supplier_nation'},
    {'$match': {'supplier_nation.N_REGIONKEY': '$nation.N_REGIONKEY'}},
    {'$group': {
        '_id': '$nation.N_NAME',
        'REVENUE': {
            '$sum': {
                '$multiply': [
                    '$lineitem.L_EXTENDEDPRICE',
                    {'$subtract': [1, '$lineitem.L_DISCOUNT']}
                ]
            }
        }
    }},
    {'$project': {
        'N_NAME': '$_id',
        'REVENUE': 1,
        '_id': 0
    }},
    {'$sort': {'REVENUE': -1}}
]

# Execute the pipeline
output = db.orders.aggregate(pipeline)

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['N_NAME', 'REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for document in output:
        writer.writerow(document)

# Clean up the client connection
client.close()
