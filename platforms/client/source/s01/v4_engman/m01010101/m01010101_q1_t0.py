from pymongo import MongoClient
import csv
from datetime import datetime

# Establish a connection to the MongoDB server
client = MongoClient('mongodb', 27017)

# Select the 'tpch' database and 'lineitem' collection
db = client['tpch']
lineitem_collection = db['lineitem']

# Perform the aggregation
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$lt': datetime(1998, 9, 2)}
        }
    },
    {
        '$group': {
            '_id': {
                'L_RETURNFLAG': '$L_RETURNFLAG',
                'L_LINESTATUS': '$L_LINESTATUS',
            },
            'L_QUANTITY': {'$sum': '$L_QUANTITY'},
            'L_EXTENDEDPRICE': {'$sum': '$L_EXTENDEDPRICE'},
            'L_DISCOUNTED_PRICE': {
                '$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}
            },
            'TAX_DISCOUNTED_PRICE': {
                '$sum': {
                    '$multiply': [
                        {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]},
                        {'$add': [1, '$L_TAX']}
                    ]
                }
            },
            'AVG_QTY': {'$avg': '$L_QUANTITY'},
            'AVG_PRICE': {'$avg': '$L_EXTENDEDPRICE'},
            'AVG_DISCOUNT': {'$avg': '$L_DISCOUNT'},
            'COUNT_ORDER': {'$sum': 1}
        }
    },
    {
        '$sort': {
            '_id.L_RETURNFLAG': 1,
            '_id.L_LINESTATUS': 1
        }
    }
]

result = list(lineitem_collection.aggregate(pipeline))

# Write to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    
    # Write the header
    writer.writerow([
        'RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE',
        'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'
    ])
    
    # Write the data
    for doc in result:
        writer.writerow([
            doc['_id']['L_RETURNFLAG'],
            doc['_id']['L_LINESTATUS'],
            doc['L_QUANTITY'],
            doc['L_EXTENDEDPRICE'],
            doc['L_DISCOUNTED_PRICE'],
            doc['TAX_DISCOUNTED_PRICE'],
            doc['AVG_QTY'],
            doc['AVG_PRICE'],
            doc['AVG_DISCOUNT'],
            doc['COUNT_ORDER']
        ])
