from pymongo import MongoClient
import csv
from datetime import datetime

# Connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Define the query
query = {
    'L_SHIPDATE': {'$lt': datetime(1998, 9, 2)},
}

# Define the projection/aggregation
projection = [
    {
        '$match': query,
    },
    {
        '$group': {
            '_id': {
                'L_RETURNFLAG': '$L_RETURNFLAG',
                'L_LINESTATUS': '$L_LINESTATUS',
            },
            'SUM_QTY': {'$sum': '$L_QUANTITY'},
            'SUM_BASE_PRICE': {'$sum': '$L_EXTENDEDPRICE'},
            'SUM_DISC_PRICE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}] }},
            'SUM_CHARGE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$multiply': [{'$subtract': [1, '$L_DISCOUNT']}, {'$add': [1, '$L_TAX']}]} ] }},
            'AVG_QTY': {'$avg': '$L_QUANTITY'},
            'AVG_PRICE': {'$avg': '$L_EXTENDEDPRICE'},
            'AVG_DISC': {'$avg': '$L_DISCOUNT'},
            'COUNT_ORDER': {'$sum': 1},
        },
    },
    {
        '$sort': {
            '_id.L_RETURNFLAG': 1,
            '_id.L_LINESTATUS': 1,
        },
    },
]

# Execute the query
results = list(lineitem_collection.aggregate(projection))

# Write to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'])

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
            result['COUNT_ORDER'],
        ])

# Close the MongoDB connection
client.close()
