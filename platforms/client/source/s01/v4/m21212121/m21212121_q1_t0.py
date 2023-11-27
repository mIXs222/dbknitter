from pymongo import MongoClient
import csv

# MongoDB connection parameters
HOST = 'mongodb'
PORT = 27017
DB_NAME = 'tpch'

# Connect to the mongoDB instance
client = MongoClient(HOST, PORT)
db = client[DB_NAME]

# Perform the aggregation equivalent to the SQL query
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$lte': '1998-09-02'}
        }
    },
    {
        '$group': {
            '_id': {
                'L_RETURNFLAG': '$L_RETURNFLAG',
                'L_LINESTATUS': '$L_LINESTATUS'
            },
            'SUM_QTY': {'$sum': '$L_QUANTITY'},
            'SUM_BASE_PRICE': {'$sum': '$L_EXTENDEDPRICE'},
            'SUM_DISC_PRICE': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            },
            'SUM_CHARGE': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE',
                        {'$subtract': [1, '$L_DISCOUNT']},
                        {'$sum': [1, '$L_TAX']}
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
            '_id.L_RETURNFLAG': 1,
            '_id.L_LINESTATUS': 1
        }
    }
]

# Execute the aggregation
results = list(db.lineitem.aggregate(pipeline))

# Write query results to query_output.csv file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # write the headers
    writer.writerow(['L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'])
    # write the data
    for result in results:
        writer.writerow([
            result['_id']['L_RETURNFLAG'],
            result['_id']['L_LINESTATUS'],
            result['SUM_QTY'],
            result['SUM_BASE_PRICE'],
            result['SUM_DISC_PRICE'],
            result['SUM_CHARGE'],
            result['AVG_QTY'],
            result['AVG_PRICE'],
            result['AVG_DISC'],
            result['COUNT_ORDER']
        ])

print("The query results have been written to query_output.csv")
