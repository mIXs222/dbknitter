from pymongo import MongoClient
import csv

# MongoDB connection string
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Query execution
orders_collection = db.orders
lineitem_collection = db.lineitem

match_stage = {
    "$match": {
        "O_ORDERDATE": {"$gte": "1993-07-01", "$lt": "1993-10-01"}
    }
}
lookup_stage = {
    "$lookup": {
        "from": "lineitem",
        "localField": "O_ORDERKEY",
        "foreignField": "L_ORDERKEY",
        "as": "lineitems"
    }
}
unwind_stage = {
    "$unwind": "$lineitems"
}
match_stage2 = {
    "$match": {
        "lineitems.L_COMMITDATE": {"$lt": "lineitems.L_RECEIPTDATE"}
    }
}
group_stage = {
    "$group": {
        "_id": "$O_ORDERPRIORITY",
        "order_count": {"$sum": 1}
    }
}
sort_stage = {
    "$sort": {"_id": 1}
}

query_pipeline = [
    match_stage,
    lookup_stage,
    unwind_stage,
    match_stage2,
    group_stage,
    sort_stage
]

results = orders_collection.aggregate(query_pipeline)

# Write results to CSV file
with open('query_output.csv', mode='w', newline='') as f:
    fieldnames = ['O_ORDERPRIORITY', 'order_count']
    writer = csv.DictWriter(f, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow({'O_ORDERPRIORITY': result["_id"], 'order_count': result["order_count"]})
