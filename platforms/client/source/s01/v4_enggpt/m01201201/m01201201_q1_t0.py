from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to the MongoDB database
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Filter for line items with a shipping date on or before September 2, 1998
query = {
    'L_SHIPDATE': {
        '$lte': datetime(1998, 9, 2)
    }
}

# Aggregation pipeline
pipeline = [
    {
        '$match': query
    }, 
    {
        '$group': {
            '_id': {
                'return_flag': '$L_RETURNFLAG',
                'line_status': '$L_LINESTATUS'
            },
            'SUM_QTY': {'$sum': '$L_QUANTITY'},
            'SUM_BASE_PRICE': {'$sum': '$L_EXTENDEDPRICE'},
            'SUM_DISC_PRICE': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE', 
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            },
            'SUM_CHARGE': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE', 
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
            '_id.return_flag': 1, 
            '_id.line_status': 1
        }
    }
]

# Run the aggregation pipeline
results = list(lineitem_collection.aggregate(pipeline))

# Write the results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    
    # Write the headers
    writer.writerow([
        'RETURN_FLAG', 'LINE_STATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 
        'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 
        'AVG_DISC', 'COUNT_ORDER'
    ])
    
    # Write the data
    for result in results:
        writer.writerow([
            result['_id']['return_flag'],
            result['_id']['line_status'],
            result['SUM_QTY'],
            result['SUM_BASE_PRICE'],
            result['SUM_DISC_PRICE'],
            result['SUM_CHARGE'],
            result['AVG_QTY'],
            result['AVG_PRICE'],
            result['AVG_DISC'],
            result['COUNT_ORDER']
        ])
