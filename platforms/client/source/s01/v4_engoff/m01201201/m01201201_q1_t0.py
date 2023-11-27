# pricing_summary_report.py

from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Define the date threshold
date_threshold = datetime(1998, 9, 2)

# Pipeline for the aggregate query
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$lt': date_threshold}
        }
    },
    {
        '$group': {
            '_id': {'RETURNFLAG': '$L_RETURNFLAG', 'LINESTATUS': '$L_LINESTATUS'},
            'TOTAL_QTY': {'$sum': '$L_QUANTITY'},
            'TOTAL_BASE_PRICE': {'$sum': '$L_EXTENDEDPRICE'},
            'TOTAL_DISC_PRICE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}] }},
            'TOTAL_CHARGE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}, {'$add': [1, '$L_TAX']}]}},
            'AVG_QTY': {'$avg': '$L_QUANTITY'},
            'AVG_PRICE': {'$avg': '$L_EXTENDEDPRICE'},
            'AVG_DISC': {'$avg': '$L_DISCOUNT'},
            'COUNT_ORDER': {'$sum': 1}
        }
    },
    {
        '$sort': {'_id.RETURNFLAG': 1, '_id.LINESTATUS': 1}
    }
]

# Execute the query
result = lineitem_collection.aggregate(pipeline)

# Write the query results to a CSV file
with open('query_output.csv', 'w', newline='') as csv_file:
    fieldnames = ['RETURNFLAG', 'LINESTATUS', 'TOTAL_QTY', 'TOTAL_BASE_PRICE', 'TOTAL_DISC_PRICE', 'TOTAL_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for item in result:
        writer.writerow({
            'RETURNFLAG': item['_id']['RETURNFLAG'],
            'LINESTATUS': item['_id']['LINESTATUS'],
            'TOTAL_QTY': item['TOTAL_QTY'],
            'TOTAL_BASE_PRICE': item['TOTAL_BASE_PRICE'],
            'TOTAL_DISC_PRICE': item['TOTAL_DISC_PRICE'],
            'TOTAL_CHARGE': item['TOTAL_CHARGE'],
            'AVG_QTY': item['AVG_QTY'],
            'AVG_PRICE': item['AVG_PRICE'],
            'AVG_DISC': item['AVG_DISC'],
            'COUNT_ORDER': item['COUNT_ORDER']
        })

print('Query results written to "query_output.csv"')
