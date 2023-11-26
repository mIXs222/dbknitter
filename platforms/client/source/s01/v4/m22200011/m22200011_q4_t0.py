from pymongo import MongoClient
import csv

# Connection to the MongoDB server
client = MongoClient('mongodb', 27017)
database = client['tpch']
orders_collection = database['orders']
lineitem_collection = database['lineitem']

# Emulating the SQL query in MongoDB
start_date = '1993-07-01'
end_date = '1993-10-01'
pipeline = [
    {
        "$match": {
            "O_ORDERDATE": {"$gte": start_date, "$lt": end_date}
        }
    },
    {
        "$lookup": {
            "from": "lineitem",
            "let": {"order_key": "$O_ORDERKEY"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$L_ORDERKEY", "$$order_key"]},
                                {"$lt": ["$L_COMMITDATE", "$L_RECEIPTDATE"]}
                            ]
                        }
                    }
                }
            ],
            "as": "matching_lineitems"
        }
    },
    {
        "$match": {
            "matching_lineitems": {"$ne": []}
        }
    },
    {
        "$group": {
            "_id": "$O_ORDERPRIORITY",
            "ORDER_COUNT": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id": 1}
    }
]
results = orders_collection.aggregate(pipeline)

# Writing results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # CSV header
    csvwriter.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
    # Write data to CSV
    for result in results:
        csvwriter.writerow([result['_id'], result['ORDER_COUNT']])
