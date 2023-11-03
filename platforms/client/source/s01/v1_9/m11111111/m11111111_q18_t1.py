import pandas as pd
from pymongo import MongoClient

# Create a connection
client = MongoClient('mongodb://localhost:27017')
db = client['tpch']

# Aggregate the data
pipeline = [
    {"$lookup": {
      "from": "orders",
      "localField": "C_CUSTKEY",
      "foreignField": "O_CUSTKEY",
      "as": "joint_orders"
    }},
    {"$unwind": "$joint_orders"},
    {"$lookup": {
      "from": "lineitem",
      "localField": "joint_orders.O_ORDERKEY",
      "foreignField": "L_ORDERKEY",
      "as": "joint_lineitems"
    }},
    {"$unwind": "$joint_lineitems"},
    {"$group": {
      "_id": {
        "C_NAME": "$C_NAME",
        "C_CUSTKEY": "$C_CUSTKEY",
        "O_ORDERKEY": "$joint_orders.O_ORDERKEY",
        "O_ORDERDATE": "$joint_orders.O_ORDERDATE",
        "O_TOTALPRICE": "$joint_orders.O_TOTALPRICE"
      },
      "SUM_QUANTITY": {"$sum": "$joint_lineitems.L_QUANTITY"}
    }},
    {"$match": {"SUM_QUANTITY": {"$gt": 300}}},
    {"$sort": {"_id.O_TOTALPRICE": -1, "_id.O_ORDERDATE": -1}}
]

# Execute the aggregation pipeline
result = db.customer.aggregate(pipeline)

# Write the result to a DataFrame and then to a csv
df = pd.DataFrame(list(result))
df.to_csv('query_output.csv')
