from pymongo import MongoClient
import csv
from datetime import datetime

def connect_to_mongodb(host, port, db_name):
    client = MongoClient(host, port)
    db = client[db_name]
    return db

def forecast_revenue_change_mongodb(db):
    # Construct the MongoDB query
    query = {
        'L_SHIPDATE': {'$gte': datetime(1994, 1, 1), '$lt': datetime(1995, 1, 1)},
        'L_DISCOUNT': {'$gte': 0.06 - 0.01, '$lte': 0.06 + 0.01},
        'L_QUANTITY': {'$lt': 24}
    }

    # Calculate potential revenue increase
    pipeline = [
        {'$match': query},
        {'$project': {'revenue_increase': {'$multiply': ['$L_EXTENDEDPRICE', '$L_DISCOUNT']}}},
        {'$group': {'_id': None, 'total_revenue_increase': {'$sum': '$revenue_increase'}}}
    ]

    # Run the aggregation pipeline
    result = list(db.lineitem.aggregate(pipeline))
    if result:
        return result[0]['total_revenue_increase']
    return 0

def save_results_to_csv(result, filename):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['total_revenue_increase']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerow({'total_revenue_increase': result})

# Connection info
host = 'mongodb'
port = 27017
db_name = 'tpch'

# Connect to MongoDB
db = connect_to_mongodb(host, port, db_name)

# Execute the query and get the result
result = forecast_revenue_change_mongodb(db)

# Write the result to CSV
save_results_to_csv(result, 'query_output.csv')
