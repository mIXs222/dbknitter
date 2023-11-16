from pymongo import MongoClient
from datetime import datetime
import csv

# pymongo connection
client = MongoClient('mongodb', 27017)
db = client['tpch']

# date condition
date_cond = datetime.strptime('1995-03-15', '%Y-%m-%d')

# MongoDB query
pipeline = [
    # join operation with orders collection
    { "$lookup": {
        "from": "orders",
        "localField": "C_CUSTKEY",
        "foreignField": "O_CUSTKEY",
        "as": "orders_info"
    }},
  
    # filter initial data
    { "$match": {
        "C_MKTSEGMENT": "BUILDING",
        "orders_info.O_ORDERDATE": { "$lt": date_cond },
        "orders_info.O_SHIPPRIORITY": { "$exists": True }
    }},

    # unwind orders_info array after join
    { "$unwind": "$orders_info" },
  
    # join operation with lineitem collection
    { "$lookup": {
        "from": "lineitem",
        "localField": "orders_info.O_ORDERKEY",
        "foreignField": "L_ORDERKEY",
        "as": "line_items"
    }},

    # filter data after join
    { "$match": {
        "line_items.L_SHIPDATE": { "$gt": date_cond }
    }},

    # unwind line_items
    { "$unwind": "$line_items" },

    # compute revenue
    { "$addFields": {
        "REVENUE": { "$multiply": ["$line_items.L_EXTENDEDPRICE", { "$subtract": [1, "$line_items.L_DISCOUNT"] }] }
    }},

    # group by fields
    { "$group": {
        "_id": {
            "L_ORDERKEY": "$line_items.L_ORDERKEY",
            "O_ORDERDATE": "$orders_info.O_ORDERDATE",
            "O_SHIPPRIORITY": "$orders_info.O_SHIPPRIORITY"
        },
        "REVENUE": { "$sum": "$REVENUE" }
    }},
    # sort operation
    { "$sort": { "REVENUE": -1, "O_ORDERDATE": 1 } }
]

# execute query
query_result = db.customer.aggregate(pipeline)

# write to csv
with open("query_output.csv", "w") as f:
    writer = csv.writer(f)
    # write header
    # assuming that each document has same keys
    writer.writerow(query_result[0].keys())
    for data in query_result:
      writer.writerow(data.values())

