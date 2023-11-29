from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to the MongoDB database
client = MongoClient("mongodb", 27017)
db = client.tpch
lineitem_collection = db.lineitem

# Convert the string date to a Python datetime object
start_date = datetime.strptime("1994-01-01", "%Y-%m-%d")
end_date = datetime.strptime("1995-01-01", "%Y-%m-%d")

# Construct the query
query = {
    'L_SHIPDATE': {'$gt': start_date, '$lt': end_date},
    'L_DISCOUNT': {'$gt': 0.05, '$lt': 0.07},
    'L_QUANTITY': {'$lt': 24},
}

# Aggregate the query results
aggregate_pipeline = [
    {'$match': query},
    {'$project': {
        'REVENUE': {'$multiply': ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]},
    }},
    {'$group': {
        '_id': None,
        'total_revenue': {'$sum': '$REVENUE'},
    }},
]

# Execute the query
result = list(lineitem_collection.aggregate(aggregate_pipeline))

# Write output to query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    if result:
        writer.writerow({'REVENUE': result[0]['total_revenue']})

# Close the MongoDB client
client.close()
