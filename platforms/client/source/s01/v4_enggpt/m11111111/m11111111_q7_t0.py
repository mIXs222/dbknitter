from pymongo import MongoClient
import csv
import datetime

# Establish a connection to the MongoDB database
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the function to perform the query
def generate_report():
    pipeline = [
        {
            "$match": {
                "L_SHIPDATE": {
                    "$gte": datetime.datetime(1995, 1, 1),
                    "$lte": datetime.datetime(1996, 12, 31)
                }
            }
        },
        {
            "$lookup": {
                "from": "orders",
                "localField": "L_ORDERKEY",
                "foreignField": "O_ORDERKEY",
                "as": "orders"
            }
        },
        {
            "$unwind": "$orders"
        },
        {
            "$lookup": {
                "from": "customer",
                "localField": "orders.O_CUSTKEY",
                "foreignField": "C_CUSTKEY",
                "as": "customers"
            }
        },
        {
            "$unwind": "$customers"
        },
        {
            "$lookup": {
                "from": "supplier",
                "localField": "L_SUPPKEY",
                "foreignField": "S_SUPPKEY",
                "as": "suppliers"
            }
        },
        {
            "$unwind": "$suppliers"
        },
        {
            "$lookup": {
                "from": "nation",
                "localField": "suppliers.S_NATIONKEY",
                "foreignField": "N_NATIONKEY",
                "as": "supplier_nations"
            }
        },
        {
            "$unwind": "$supplier_nations"
        },
        {
            "$lookup": {
                "from": "nation",
                "localField": "customers.C_NATIONKEY",
                "foreignField": "N_NATIONKEY",
                "as": "customer_nations"
            }
        },
        {
            "$unwind": "$customer_nations"
        },
        {
            "$match": {
                "$or": [
                    {
                        "supplier_nations.N_NAME": "JAPAN",
                        "customer_nations.N_NAME": "INDIA"
                    },
                    {
                        "supplier_nations.N_NAME": "INDIA",
                        "customer_nations.N_NAME": "JAPAN"
                    }
                ]
            }
        },
        {
            "$project": {
                "supplier_nation": "$supplier_nations.N_NAME",
                "customer_nation": "$customer_nations.N_NAME",
                "year": {"$year": "$L_SHIPDATE"},
                "revenue": {
                    "$multiply": [
                        "$L_EXTENDEDPRICE",
                        {"$subtract": [1, "$L_DISCOUNT"]}
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "supplier_nation": "$supplier_nation",
                    "customer_nation": "$customer_nation",
                    "year": "$year"
                },
                "total_revenue": {"$sum": "$revenue"}
            }
        },
        {
            "$sort": {
                "_id.supplier_nation": 1,
                "_id.customer_nation": 1,
                "_id.year": 1
            }
        }
    ]

    results = db.lineitem.aggregate(pipeline)

    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['supplier_nation', 'customer_nation', 'year', 'revenue']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for doc in results:
            writer.writerow({
                'supplier_nation': doc['_id']['supplier_nation'],
                'customer_nation': doc['_id']['customer_nation'],
                'year': doc['_id']['year'],
                'revenue': doc['total_revenue'],
            })

# Call the function to execute the query and write the results to CSV
generate_report()
