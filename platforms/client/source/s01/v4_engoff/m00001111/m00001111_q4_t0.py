import csv
from datetime import datetime
from pymongo import MongoClient

# Connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Query orders and lineitems
start_date = datetime(1993, 7, 1)
end_date = datetime(1993, 10, 1)

orders_with_late_lineitems = db.orders.aggregate([
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
            "O_ORDERDATE": {
                "$gte": start_date,
                "$lt": end_date
            },
            "lineitems": {
                "$elemMatch": {
                    "L_RECEIPTDATE": {
                        "$gt": "$$ROOT.L_COMMITDATE"
                    }
                }
            }
        }
    },
    {
        "$group": {
            "_id": "$O_ORDERPRIORITY",
            "count": {"$count": {}}
        }
    },
    {
        "$sort": {"_id": 1}
    }
])

# Write output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['O_ORDERPRIORITY', 'order_count'])
    for doc in orders_with_late_lineitems:
        writer.writerow([doc['_id'], doc['count']])

print("Query output has been written to query_output.csv.")
