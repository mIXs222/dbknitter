import pymongo
import csv
from datetime import datetime

# Connect to the MongoDB server
client = pymongo.MongoClient("mongodb", 27017)
db = client['tpch']

# Convert SQL 'where' clause to MongoDB query format
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)
query = {
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
}

# Perform the aggregation query
pipeline = [
    {'$match': query},
    {'$project': {
        'L_SUPPKEY': 1,
        'TOTAL_REVENUE': {
            '$multiply': [
                '$L_EXTENDEDPRICE',
                {'$subtract': [1, '$L_DISCOUNT']}
            ]
        }
    }},
    {'$group': {
        '_id': '$L_SUPPKEY',
        'TOTAL_REVENUE': {'$sum': '$TOTAL_REVENUE'}
    }},
    {'$sort': {'TOTAL_REVENUE': -1}},
    {'$limit': 1}
]

# Execute pipeline on lineitem collection
revenue0 = list(db.lineitem.aggregate(pipeline))

# Query suppliers
supplier_no = revenue0[0]['_id'] if revenue0 else None
supplier_info = None

if supplier_no:
    supplier_info = db.supplier.find_one({'S_SUPPKEY': supplier_no}, {'_id': 0})

# Merge the results and write to a CSV file
if supplier_info and revenue0:
    result = {**supplier_info, **revenue0[0]}
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(result)

print("Query results have been successfully written to query_output.csv.")
