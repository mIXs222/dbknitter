from pymongo import MongoClient
import csv

def get_orders_and_lineitem_data():
    client = MongoClient("mongodb://mongodb:27017")
    db = client["tpch"]

    orders_coll = db['orders']
    lineitem_coll = db['lineitem']

    results = orders_coll.aggregate([
        {"$match": {
            "O_ORDERDATE": {
                "$gte": "1993-07-01",
                "$lt": "1993-10-01"
            }
        }},
        {"$lookup": {
            "from": "lineitem",
            "let": {"order_id": "$O_ORDERKEY"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$L_ORDERKEY", "$$order_id"]},
                            {"$lt": ["$L_COMMITDATE", "$L_RECEIPTDATE"]}
                        ]
                    }
                }}
            ],
            "as": "order_lineitems"
        }},
        {"$unwind": "$order_lineitems"},
        {"$group": {
            "_id": "$O_ORDERPRIORITY",
            "ORDER_COUNT": {"$sum": 1}
        }},
        {"$sort": {
            "_id": 1
        }}
    ])

    return results

def write_to_csv(data):
    with open('query_output.csv', 'w') as file:
        writer = csv.DictWriter(file, fieldnames=["O_ORDERPRIORITY", "ORDER_COUNT"])
        writer.writeheader()
        for row in data:
            writer.writerow({"O_ORDERPRIORITY": row["_id"], "ORDER_COUNT": row["ORDER_COUNT"]})

if __name__ == "__main__":
    data = get_orders_and_lineitem_data()
    write_to_csv(data)
