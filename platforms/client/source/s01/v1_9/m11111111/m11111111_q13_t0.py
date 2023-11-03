from pymongo import MongoClient
import pandas as pd

# Setup MongoDB client
client = MongoClient("mongodb://mongodb:27017/")
database = client["tpch"]
customer = database["customer"]
orders = database["orders"]

# Aggregate pipeline
pipeline = [
    {"$lookup": {
        "from": "orders",
        "let": {"c_custkey": "$C_CUSTKEY"},
        "pipeline": [
            {"$match":
                {"$expr":
                    {"$and":
                        [
                            {"$eq": ["$O_CUSTKEY", "$$c_custkey"]},
                            {"$not": {"$regex": ".*pending.*deposits.*"}}
                        ]
                    }
                }
            }
        ],
        "as": "order_list"
    }},
    {"$unwind": "$order_list"},
    {"$group": {
        "_id": "$C_CUSTKEY",
        "C_COUNT": {"$sum": 1}
    }},
    {"$group": {
        "_id": "$C_COUNT",
        "CUSTDIST": {"$sum": 1}
    }},
    {"$sort": {
        "CUSTDIST":-1, 
        "C_COUNT":-1
    }},
]

# Execute the aggregation
result = customer.aggregate(pipeline)

# Convert to a data frame
df = pd.DataFrame(list(result))

# Write to a CSV
df.to_csv("query_output.csv", index=False)
