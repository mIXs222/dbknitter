from pymongo import MongoClient
import csv

# MongoDB connection parameters
host = "mongodb"
port = 27017
dbname = "tpch"
collection_name = "lineitem"

client = MongoClient(host, port)
db = client[dbname]
collection = db[collection_name]

# MongoDB aggregation pipeline equivalent to the SQL query
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {"$lte": "1998-09-02"}
        }
    },
    {
        "$group": {
            "_id": {
                "L_RETURNFLAG": "$L_RETURNFLAG",
                "L_LINESTATUS": "$L_LINESTATUS"
            },
            "SUM_QTY": {"$sum": "$L_QUANTITY"},
            "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
            "SUM_DISC_PRICE": {
                "$sum": {
                    "$multiply": [
                        "$L_EXTENDEDPRICE",
                        {"$subtract": [1, "$L_DISCOUNT"]}
                    ]
                }
            },
            "SUM_CHARGE": {
                "$sum": {
                    "$multiply": [
                        "$L_EXTENDEDPRICE",
                        {
                            "$multiply": [
                                {"$subtract": [1, "$L_DISCOUNT"]},
                                {"$add": [1, "$L_TAX"]}
                            ]
                        }
                    ]
                }
            },
            "AVG_QTY": {"$avg": "$L_QUANTITY"},
            "AVG_PRICE": {"$avg": "$L_EXTENDEDPRICE"},
            "AVG_DISC": {"$avg": "$L_DISCOUNT"},
            "COUNT_ORDER": {"$sum": 1}
        }
    },
    {
        "$sort": {
            "_id.L_RETURNFLAG": 1,
            "_id.L_LINESTATUS": 1
        }
    }
]

# Execute the aggregation query
results = list(collection.aggregate(pipeline))

# Writing results to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    fieldnames = [
        'L_RETURNFLAG',
        'L_LINESTATUS',
        'SUM_QTY',
        'SUM_BASE_PRICE',
        'SUM_DISC_PRICE',
        'SUM_CHARGE',
        'AVG_QTY',
        'AVG_PRICE',
        'AVG_DISC',
        'COUNT_ORDER'
    ]
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    for result in results:
        writer.writerow({
            'L_RETURNFLAG': result['_id']['L_RETURNFLAG'],
            'L_LINESTATUS': result['_id']['L_LINESTATUS'],
            'SUM_QTY': result['SUM_QTY'],
            'SUM_BASE_PRICE': result['SUM_BASE_PRICE'],
            'SUM_DISC_PRICE': result['SUM_DISC_PRICE'],
            'SUM_CHARGE': result['SUM_CHARGE'],
            'AVG_QTY': result['AVG_QTY'],
            'AVG_PRICE': result['AVG_PRICE'],
            'AVG_DISC': result['AVG_DISC'],
            'COUNT_ORDER': result['COUNT_ORDER']
        })

print("Query results have been written to query_output.csv")
