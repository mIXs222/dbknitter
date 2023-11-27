# large_volume_customer_query.py
from pymongo import MongoClient
import csv

def large_volume_customer_query(mongo_host, mongo_port, mongo_db, output_file):
    try:
        # Connect to MongoDB
        client = MongoClient(host=mongo_host, port=mongo_port)
        db = client[mongo_db]

        # Perform aggregation to find large volume orders and retrieve customer details
        pipeline = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "C_CUSTKEY",
                    "foreignField": "O_CUSTKEY",
                    "as": "order_details"
                }
            },
            {"$unwind": "$order_details"},
            {
                "$lookup": {
                    "from": "lineitem",
                    "localField": "order_details.O_ORDERKEY",
                    "foreignField": "L_ORDERKEY",
                    "as": "lineitem_details"
                }
            },
            {"$unwind": "$lineitem_details"},
            {
                "$group": {
                    "_id": {
                        "C_CUSTKEY": "$C_CUSTKEY",
                        "C_NAME": "$C_NAME",
                        "O_ORDERKEY": "$order_details.O_ORDERKEY",
                        "O_ORDERDATE": "$order_details.O_ORDERDATE",
                        "O_TOTALPRICE": "$order_details.O_TOTALPRICE"
                    },
                    "total_quantity": {
                        "$sum": "$lineitem_details.L_QUANTITY"
                    }
                }
            },
            {
                "$match": {
                    "total_quantity": {"$gt": 300}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "customer_key": "$_id.C_CUSTKEY",
                    "customer_name": "$_id.C_NAME",
                    "order_key": "$_id.O_ORDERKEY",
                    "order_date": "$_id.O_ORDERDATE",
                    "total_price": "$_id.O_TOTALPRICE",
                    "quantity": "$total_quantity"
                }
            }
        ]
        
        results = db.customer.aggregate(pipeline)
        
        # Write results to file
        with open(output_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["customer_name", "customer_key", "order_key", "order_date", "total_price", "quantity"])
            for result in results:
                writer.writerow([
                    result['customer_name'],
                    result['customer_key'],
                    result['order_key'],
                    result['order_date'],
                    result['total_price'],
                    result['quantity']
                ])

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    mongo_host = 'mongodb'
    mongo_port = 27017
    mongo_db = 'tpch'
    output_file = 'query_output.csv'
    large_volume_customer_query(mongo_host, mongo_port, mongo_db, output_file)
