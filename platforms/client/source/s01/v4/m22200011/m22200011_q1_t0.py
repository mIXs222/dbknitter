# query_code.py

from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch
lineitem_collection = db.lineitem

# Perform the aggregation query
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
            'SUM_DISC_PRICE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}] }},
            'SUM_CHARGE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}, {'$add': [1, '$L_TAX']}]}},
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

result = lineitem_collection.aggregate(pipeline)

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for item in result:
        writer.writerow({
            'L_RETURNFLAG': item['_id']['L_RETURNFLAG'],
            'L_LINESTATUS': item['_id']['L_LINESTATUS'],
            'SUM_QTY': item['SUM_QTY'],
            'SUM_BASE_PRICE': item['SUM_BASE_PRICE'],
            'SUM_DISC_PRICE': item['SUM_DISC_PRICE'],
            'SUM_CHARGE': item['SUM_CHARGE'],
            'AVG_QTY': item['AVG_QTY'],
            'AVG_PRICE': item['AVG_PRICE'],
            'AVG_DISC': item['AVG_DISC'],
            'COUNT_ORDER': item['COUNT_ORDER']
        })
