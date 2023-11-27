from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://localhost:27017/")
db = client['tpch']
customer = db['customer']
orders = db['orders']

# Aggregate command is used, which is similar to JOIN in SQL.
pipeline = [
    {"$lookup":
        {
        "from": "orders",
        "localField": "C_CUSTKEY",
        "foreignField": "O_CUSTKEY",
        "as": "customer_orders"
        }
    },
    {"$unwind": "$customer_orders"},
    {"$match": {"customer_orders.O_COMMENT": {"$not": {"$regex": "pending|deposits"}}}},
    {"$group": {"_id": "$C_CUSTKEY", "C_COUNT": {"$sum": 1}}},
    {"$group": {"_id": "$C_COUNT", "CUSTDIST": {"$sum": 1}}},
    {"$sort": {"CUSTDIST": -1, "_id": -1}}
]

result = list(db.customer.aggregate(pipeline))

# convert result to dataframe and write it to .csv
df = pd.DataFrame(result)
df.to_csv('query_output.csv', index=False)
