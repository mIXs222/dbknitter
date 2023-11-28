import pymongo
import csv
from datetime import datetime

# Connection to MongoDB
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# Define the date range
start_date = datetime.strptime('1993-07-01', '%Y-%m-%d')
end_date = datetime.strptime('1993-10-01', '%Y-%m-%d')

# Aggregation pipeline
pipeline = [
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "lineitems"
        }
    },
    {
        "$match": {
            "O_ORDERDATE": {"$gte": start_date, "$lt": end_date},
            "lineitems": {
                "$elemMatch": {
                    "L_COMMITDATE": {"$lt": "$$ROOT.lineitems.L_RECEIPTDATE"}
                }
            }
        }
    },
    {
        "$group": {
            "_id": "$O_ORDERPRIORITY",
            "order_count": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id": 1}
    }
]

results = db.orders.aggregate(pipeline)
output_file = 'query_output.csv'

# Write results to CSV
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
    for result in results:
        writer.writerow([result['_id'], result['order_count']])

print(f"Query results written to {output_file}")
