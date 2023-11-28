from pymongo import MongoClient
import csv

# Function to calculate the total quantity per order.
def get_orders_with_quantity_over_threshold(db, threshold=300):
    pipeline = [
        {"$group": {
            "_id": "$L_ORDERKEY",
            "total_quantity": {"$sum": "$L_QUANTITY"}
        }},
        {"$match": {
            "total_quantity": {"$gt": threshold}
        }}
    ]
    return set(d['_id'] for d in db.lineitem.aggregate(pipeline))

# Function to fetch and write the analyzed data.
def fetch_and_write_data(db, order_keys_with_high_quantity):
    pipeline = [
        {"$match": {
            "O_ORDERKEY": {"$in": list(order_keys_with_high_quantity)}
        }},
        {"$lookup": {
            "from": "customer",
            "localField": "O_CUSTKEY",
            "foreignField": "C_CUSTKEY",
            "as": "customer_info"
        }},
        {"$unwind": "$customer_info"},
        {"$lookup": {
            "from": "lineitem",
            "localField": "O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "lineitems"
        }},
        {"$project": {
            "customer_name": "$customer_info.C_NAME",
            "customer_key": "$O_CUSTKEY",
            "order_key": "$O_ORDERKEY",
            "order_date": "$O_ORDERDATE",
            "total_price": "$O_TOTALPRICE",
            "total_quantity": {"$sum": "$lineitems.L_QUANTITY"}
        }},
        {"$sort": {"total_price": -1, "order_date": 1}}
    ]

    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['customer_name', 'customer_key', 'order_key', 'order_date', 'total_price', 'total_quantity']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for record in db.orders.aggregate(pipeline):
            writer.writerow(record)

# Connect to MongoDB.
client = MongoClient('mongodb', 27017)
db = client.tpch

# Get data and write to CSV.
order_keys_with_high_quantity = get_orders_with_quantity_over_threshold(db)
fetch_and_write_data(db, order_keys_with_high_quantity)
