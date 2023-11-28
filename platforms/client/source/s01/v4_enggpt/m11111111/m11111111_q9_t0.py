import pymongo
from datetime import datetime
import csv

# MongoDB connection
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# Aggregation pipeline
pipeline = [
    {
        '$match': {
            'P_NAME': {'$regex': '.*dim.*', '$options': 'i'}
        }
    },
    {
        '$lookup': {
            'from': 'partsupp',
            'localField': 'P_PARTKEY',
            'foreignField': 'PS_PARTKEY',
            'as': 'partsupp'
        }
    },
    {'$unwind': '$partsupp'},
    {
        '$lookup': {
            'from': 'lineitem',
            'let': {'partKey': '$P_PARTKEY', 'suppKey': '$partsupp.PS_SUPPKEY'},
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {'$eq': ['$L_PARTKEY', '$$partKey']},
                                {'$eq': ['$L_SUPPKEY', '$$suppKey']},
                            ]
                        }
                    }
                }
            ],
            'as': 'lineitem'
        }
    },
    {'$unwind': '$lineitem'},
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'lineitem.L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'orders'
        }
    },
    {'$unwind': '$orders'},
    {
        '$lookup': {
            'from': 'supplier',
            'localField': 'partsupp.PS_SUPPKEY',
            'foreignField': 'S_SUPPKEY',
            'as': 'supplier'
        }
    },
    {'$unwind': '$supplier'},
    {
        '$lookup': {
            'from': 'nation',
            'localField': 'supplier.S_NATIONKEY',
            'foreignField': 'N_NATIONKEY',
            'as': 'nation'
        }
    },
    {'$unwind': '$nation'},
    {
        '$project': {
            'N_NAME': '$nation.N_NAME',
            'O_YEAR': {'$year': '$orders.O_ORDERDATE'},
            'PROFIT': {
                '$subtract': [
                    {'$multiply': [
                        '$lineitem.L_EXTENDEDPRICE',
                        {'$subtract': [1, '$lineitem.L_DISCOUNT']}
                    ]},
                    {'$multiply': ['$partsupp.PS_SUPPLYCOST', '$lineitem.L_QUANTITY']}
                ]
            }
        }
    },
    {
        '$group': {
            '_id': {'NATION': '$N_NAME', 'YEAR': '$O_YEAR'},
            'TOTAL_PROFIT': {'$sum': '$PROFIT'}
        }
    },
    {
        '$sort': {'_id.NATION': 1, '_id.YEAR': -1}
    }
]

# Run aggregation
results = db['part'].aggregate(pipeline)

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['NATION', 'YEAR', 'TOTAL_PROFIT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow({
            'NATION': result['_id']['NATION'],
            'YEAR': result['_id']['YEAR'],
            'TOTAL_PROFIT': result['TOTAL_PROFIT']
        })

# Close connection
client.close()
