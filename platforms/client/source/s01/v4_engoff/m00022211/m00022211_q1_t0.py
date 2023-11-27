from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
database = client['tpch']
lineitem_collection = database['lineitem']

# Perform the query
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$lt': datetime(1998, 9, 2)}
        }
    },
    {
        '$group': {
            '_id': {
                'RETURNFLAG': '$L_RETURNFLAG',
                'LINESTATUS': '$L_LINESTATUS'
            },
            'SUM_QTY': {'$sum': '$L_QUANTITY'},
            'SUM_BASE_PRICE': {'$sum': '$L_EXTENDEDPRICE'},
            'SUM_DISC_PRICE': {
                '$sum': {
                    '$multiply': [
                        '$L_QUANTITY', 
                        '$L_EXTENDEDPRICE', 
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            },
            'SUM_CHARGE': {
                '$sum': {
                    '$multiply': [
                        '$L_QUANTITY',
                        '$L_EXTENDEDPRICE',
                        {'$subtract': [1, '$L_DISCOUNT']},
                        {'$add': [1, '$L_TAX']}
                    ]
                }
            },
            'AVG_QTY': {'$avg': '$L_QUANTITY'},
            'AVG_PRICE': {'$avg': '$L_EXTENDEDPRICE'},
            'AVG_DISC': {'$avg': '$L_DISCOUNT'},
            'COUNT_ORDER': {'$sum': 1}
        }
    },
    {
        '$sort': {
            '_id.RETURNFLAG': 1,
            '_id.LINESTATUS': 1
        }
    }
]

results = list(lineitem_collection.aggregate(pipeline))

# Write the output to the CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow([
        'RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 
        'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 
        'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'
    ])
    
    for result in results:
        writer.writerow([
            result['_id']['RETURNFLAG'],
            result['_id']['LINESTATUS'],
            result['SUM_QTY'],
            result['SUM_BASE_PRICE'],
            result['SUM_DISC_PRICE'],
            result['SUM_CHARGE'],
            result['AVG_QTY'],
            result['AVG_PRICE'],
            result['AVG_DISC'],
            result['COUNT_ORDER']
        ])

# Close the MongoDB connection
client.close()
