from pymongo import MongoClient
import csv
import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Query
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$lt': datetime.datetime(1998, 9, 2)}
        }
    },
    {
        '$group': {
            '_id': {'RETURNFLAG': '$L_RETURNFLAG', 'LINESTATUS': '$L_LINESTATUS'},
            'SUM_QTY': {'$sum': '$L_QUANTITY'},
            'SUM_BASE_PRICE': {'$sum': '$L_EXTENDEDPRICE'},
            'SUM_DISC_PRICE': {
                '$sum': {
                    '$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]
                }
            },
            'SUM_CHARGE': {
                '$sum': {
                    '$multiply': [
                        {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]},
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

results = lineitem_collection.aggregate(pipeline)

# Write query results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = [
        'RETURNFLAG',
        'LINESTATUS',
        'SUM_QTY',
        'SUM_BASE_PRICE',
        'SUM_DISC_PRICE',
        'SUM_CHARGE',
        'AVG_QTY',
        'AVG_PRICE',
        'AVG_DISC',
        'COUNT_ORDER'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        row = {
            'RETURNFLAG': result['_id']['RETURNFLAG'],
            'LINESTATUS': result['_id']['LINESTATUS'],
            'SUM_QTY': result['SUM_QTY'],
            'SUM_BASE_PRICE': result['SUM_BASE_PRICE'],
            'SUM_DISC_PRICE': result['SUM_DISC_PRICE'],
            'SUM_CHARGE': result['SUM_CHARGE'],
            'AVG_QTY': result['AVG_QTY'],
            'AVG_PRICE': result['AVG_PRICE'],
            'AVG_DISC': result['AVG_DISC'],
            'COUNT_ORDER': result['COUNT_ORDER']
        }
        writer.writerow(row)

client.close()
