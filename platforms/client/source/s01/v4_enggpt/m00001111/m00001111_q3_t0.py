import pymongo
import csv
from datetime import datetime

def connect_mongodb(host, port, db_name):
    client = pymongo.MongoClient(host, port)
    return client[db_name]

def convert_to_csv(data, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'REVENUE'])
        for line in data:
            writer.writerow(line)

def main():
    host = 'mongodb'
    port = 27017
    db_name = 'tpch'
    db = connect_mongodb(host, port, db_name)

    customers = db['customer']
    orders = db['orders']
    lineitems = db['lineitem']

    # Query stage
    customer_matches = customers.find({"C_MKTSEGMENT": "BUILDING"}, {"_id": 0, "C_CUSTKEY": 1})
    customer_keys = [cust['C_CUSTKEY'] for cust in customer_matches]

    order_matches = orders.find(
        {"O_CUSTKEY": {"$in": customer_keys}, "O_ORDERDATE": {"$lt": datetime(1995, 3, 15)}},
        {"_id": 0, "O_ORDERKEY": 1, "O_ORDERDATE": 1, "O_SHIPPRIORITY": 1}
    )

    results = []
    for order in order_matches:
        order_key = order['O_ORDERKEY']
        lineitem_matches = lineitems.aggregate([
            {"$match": {"L_ORDERKEY": order_key, "L_SHIPDATE": {"$gt": datetime(1995, 3, 15)}}},
            {"$project": {
                "revenue": {"$subtract": ["$L_EXTENDEDPRICE", {"$multiply": ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]}]}
            }},
            {"$group": {
                "_id": None,
                "total_revenue": {"$sum": "$revenue"}
            }}
        ])
        for lineitem in lineitem_matches:
            results.append([order['O_ORDERKEY'], order['O_ORDERDATE'].strftime('%Y-%m-%d'),
                            order['O_SHIPPRIORITY'], lineitem['total_revenue']])

    # Sort results
    results.sort(key=lambda x: (-x[3], x[1]))

    # Write to CSV
    convert_to_csv(results, 'query_output.csv')

if __name__ == "__main__":
    main()
