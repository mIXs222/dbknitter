import csv
from datetime import datetime
from pymongo import MongoClient

# Function to connect to the mongodb instance
def connect_mongodb(host, port, db_name):
    client = MongoClient(host, port)
    db = client[db_name]
    return db

# Function to perform aggregation in MongoDB
def query_mongodb(db):
    pipeline = [
        {
            "$match": {
                "L_SHIPDATE": {"$lte": datetime(1998, 9, 2)}
            }
        },
        {
            "$group": {
                "_id": {"L_RETURNFLAG": "$L_RETURNFLAG", "L_LINESTATUS": "$L_LINESTATUS"},
                "SUM_QTY": {"$sum": "$L_QUANTITY"},
                "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
                "SUM_DISC_PRICE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}},
                "SUM_CHARGE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}, {"$add": [1, "$L_TAX"]}]}},
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
    
    return list(db.lineitem.aggregate(pipeline))

# Connecting to the MongoDB
db = connect_mongodb('mongodb', 27017, 'tpch')

# Executing the query
result = query_mongodb(db)

# Writing the query result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE',
                  'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE',
                  'AVG_DISC', 'COUNT_ORDER']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for doc in result:
        writer.writerow({
            'L_RETURNFLAG': doc['_id']['L_RETURNFLAG'],
            'L_LINESTATUS': doc['_id']['L_LINESTATUS'],
            'SUM_QTY': doc['SUM_QTY'],
            'SUM_BASE_PRICE': doc['SUM_BASE_PRICE'],
            'SUM_DISC_PRICE': doc['SUM_DISC_PRICE'],
            'SUM_CHARGE': doc['SUM_CHARGE'],
            'AVG_QTY': doc['AVG_QTY'],
            'AVG_PRICE': doc['AVG_PRICE'],
            'AVG_DISC': doc['AVG_DISC'],
            'COUNT_ORDER': doc['COUNT_ORDER']
        })
