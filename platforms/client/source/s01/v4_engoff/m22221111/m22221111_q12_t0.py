import pymongo
import csv
from datetime import datetime

mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
tpch_db = mongo_client['tpch']

# Assuming all relevant tables are in the MongoDB
orders_col = tpch_db['orders']
lineitem_col = tpch_db['lineitem']

# Specify the receipt date range
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)

pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'order_info'
        }
    },
    {'$unwind': '$order_info'},
    {
        '$match': {
            'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
            'L_RECEIPTDATE': {'$gte': start_date, '$lt': end_date},
            'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
            'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'},
            'order_info.O_ORDERPRIORITY': {'$in': ['URGENT', 'HIGH']}
        }
    },
    {
        '$group': {
            '_id': '$L_SHIPMODE',
            'late_order_count_priority': {'$sum': 1}
        }
    }
]

# Stage to get late orders with priority other than URGENT or HIGH
pipeline_non_priority = [
    stage for stage in pipeline
]
pipeline_non_priority[-2]['$match']['order_info.O_ORDERPRIORITY'] = {'$nin': ['URGENT', 'HIGH']}

priority_results = list(lineitem_col.aggregate(pipeline))
non_priority_results = list(lineitem_col.aggregate(pipeline_non_priority))

# Combine results into one dictionary
combined_results = {}
for ship_mode in ['MAIL', 'SHIP']:
    combined_results[ship_mode] = {
        'late_order_count_priority': 0,
        'late_order_count_non_priority': 0
    }

for result in priority_results:
    if result['_id'] in combined_results:
        combined_results[result['_id']]['late_order_count_priority'] = result['late_order_count_priority']

for result in non_priority_results:
    if result['_id'] in combined_results:
        combined_results[result['_id']]['late_order_count_non_priority'] = result['late_order_count_priority']

# Writing the results to a csv file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['ship_mode', 'late_order_count_priority', 'late_order_count_non_priority']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for ship_mode, counts in combined_results.items():
        writer.writerow({
            'ship_mode': ship_mode,
            'late_order_count_priority': counts['late_order_count_priority'],
            'late_order_count_non_priority': counts['late_order_count_non_priority']
        })
