from pymongo import MongoClient
import pandas as pd
import datetime

# Connection to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['tpch']

# Running query
pipeline = [
    {
        "$lookup": {
            "from": "orders",
            "localField": "C_CUSTKEY",
            "foreignField": "O_CUSTKEY",
            "as": "from_orders"
        }
    },
    {"$unwind": "$from_orders"},
    {
        "$match": {
            "from_orders.O_ORDERDATE": {"$gte": datetime.datetime(1993, 10, 1), "$lt": datetime.datetime(1994, 1, 1)}
        }
    },
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "from_orders.O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "from_lineitems"
        }
    },
    {"$unwind": "$from_lineitems"},
    {
        "$match": {
            "from_lineitems.L_RETURNFLAG": "R"
        }
    },
    {
        "$lookup": {
            "from": "nation",
            "localField": "C_NATIONKEY",
            "foreignField": "N_NATIONKEY",
            "as": "from_nation"
        }
    },
    {"$unwind": "$from_nation"},
    {
        "$group": {
            "_id": {
                "C_CUSTKEY": "$C_CUSTKEY", 
                "C_NAME": "$C_NAME",
                "C_ACCTBAL": "$C_ACCTBAL",
                "C_PHONE": "$C_PHONE",
                "N_NAME": "$from_nation.N_NAME",
                "C_ADDRESS": "$C_ADDRESS",
                "C_COMMENT": "$C_COMMENT",
            },
            "REVENUE": {
                "$sum": {
                    "$multiply": ["$from_lineitems.L_EXTENDEDPRICE", {"$subtract": [1, "$from_lineitems.L_DISCOUNT"]}]
                }
            }
        }
    },
    {"$sort": {"REVENUE": 1, "_id.C_CUSTKEY": 1, "_id.C_NAME": 1, "_id.C_ACCTBAL": -1}}
]

result = db.customer.aggregate(pipeline)

# Converting cursor to list 
res_list = list(result)

# Creating a DataFrame from list of dictionaries 
df = pd.DataFrame(res_list)

# Writing DataFrame to CSV
df.to_csv('query_output.csv', index=False)
