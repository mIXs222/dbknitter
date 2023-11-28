import pymongo
import csv
from datetime import datetime

def query_mongodb():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client["tpch"]
    supplier = db["supplier"]
    lineitem = db["lineitem"]

    start_date = datetime(1996, 1, 1)
    end_date = datetime(1996, 3, 31)

    # Create the 'revenue0' equivalent in MongoDB
    pipeline = [
        {
            "$match": {
                "L_SHIPDATE": {"$gte": start_date, "$lte": end_date}
            }
        },
        {
            "$group": {
                "_id": "$L_SUPPKEY",
                "total_revenue": {
                    "$sum": {
                        "$multiply": [
                            "$L_EXTENDEDPRICE",
                            {"$subtract": [1, "$L_DISCOUNT"]}
                        ]
                    }
                }
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": 1}
    ]

    max_revenue_supplier = list(lineitem.aggregate(pipeline))[0]

    # Get the supplier details
    supplier_details = supplier.find_one({"S_SUPPKEY": max_revenue_supplier['_id']})

    return {
        "S_SUPPKEY": supplier_details["S_SUPPKEY"],
        "S_NAME": supplier_details["S_NAME"],
        "S_ADDRESS": supplier_details["S_ADDRESS"],
        "S_PHONE": supplier_details["S_PHONE"],
        "total_revenue": max_revenue_supplier["total_revenue"]
    }

def write_to_csv(data):
    with open('query_output.csv', 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=data.keys())
        writer.writeheader()
        writer.writerow(data)

if __name__ == "__main__":
    max_revenue_data = query_mongodb()
    write_to_csv(max_revenue_data)
