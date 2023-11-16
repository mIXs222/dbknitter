from pymongo import MongoClient
import csv
from datetime import datetime

# Connection to MongoDB
client = MongoClient(host='mongodb', port=27017)
db = client['tpch']

# Aggregation pipeline to simulate the SQL query
pipeline = [
    {
        "$match": {
            "P_NAME": {"$regex": ".*dim.*"}
        }
    },
    {
        "$lookup": {
            "from": "partsupp",
            "let": {"partkey": "$P_PARTKEY"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$eq": ["$PS_PARTKEY", "$$partkey"]
                        }
                    }
                }
            ],
            "as": "partsupp_docs"
        }
    },
    {
        "$unwind": "$partsupp_docs"
    },
    {
        "$lookup": {
            "from": "lineitem",
            "let": {
                "partkey": "$P_PARTKEY",
                "suppkey": "$partsupp_docs.PS_SUPPKEY"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$L_PARTKEY", "$$partkey"]},
                                {"$eq": ["$L_SUPPKEY", "$$suppkey"]}
                            ]
                        }
                    }
                }
            ],
            "as": "lineitem_docs"
        }
    },
    {
        "$unwind": "$lineitem_docs"
    },
    {
        "$lookup": {
            "from": "orders",
            "localField": "lineitem_docs.L_ORDERKEY",
            "foreignField": "O_ORDERKEY",
            "as": "orders_docs"
        }
    },
    {
        "$unwind": "$orders_docs"
    },
    {
        "$lookup": {
            "from": "supplier",
            "localField": "partsupp_docs.PS_SUPPKEY",
            "foreignField": "S_SUPPKEY",
            "as": "supplier_docs"
        }
    },
    {
        "$unwind": "$supplier_docs"
    },
    {
        "$lookup": {
            "from": "nation",
            "localField": "supplier_docs.S_NATIONKEY",
            "foreignField": "N_NATIONKEY",
            "as": "nation_docs"
        }
    },
    {
        "$unwind": "$nation_docs"
    },
    {
        "$project": {
            "NATION": "$nation_docs.N_NAME",
            "O_YEAR": {"$year": "$orders_docs.O_ORDERDATE"},
            "AMOUNT": {
                "$subtract": [
                    {"$multiply": ["$lineitem_docs.L_EXTENDEDPRICE", {"$subtract": [1, "$lineitem_docs.L_DISCOUNT"]}]},
                    {"$multiply": ["$partsupp_docs.PS_SUPPLYCOST", "$lineitem_docs.L_QUANTITY"]}
                ]
            }
        }
    },
    {
        "$group": {
            "_id": {"NATION": "$NATION", "O_YEAR": "$O_YEAR"},
            "SUM_PROFIT": {"$sum": "$AMOUNT"}
        }
    },
    {
        "$sort": {"_id.NATION": 1, "_id.O_YEAR": -1}
    }
]

# Executing aggregation pipeline
results = list(db.part.aggregate(pipeline))

# Writing the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['NATION', 'O_YEAR', 'SUM_PROFIT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for result in results:
        writer.writerow({
            'NATION': result['_id']['NATION'],
            'O_YEAR': result['_id']['O_YEAR'],
            'SUM_PROFIT': result['SUM_PROFIT']
        })

# Close MongoDB connection
client.close()
