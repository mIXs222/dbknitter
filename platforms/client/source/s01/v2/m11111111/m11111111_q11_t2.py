from pymongo import MongoClient
import pandas as pd
from bson.son import SON

client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

pipeline = [
    {"$match": {"N_NAME": "GERMANY"}},
    {"$lookup": {
        "from": "supplier",
        "localField": "N_NATIONKEY",
        "foreignField": "S_NATIONKEY",
        "as": "supplier"
    }},
    {"$unwind": "$supplier"},
    {"$lookup": {
        "from": "partsupp",
        "localField": "supplier.S_SUPPKEY",
        "foreignField": "PS_SUPPKEY",
        "as": "partsupp"
    }},
    {"$unwind": "$partsupp"},
    {"$group": {
        "_id": "$partsupp.PS_PARTKEY",
        "VALUE": {
            "$sum": {
                "$multiply": ["$partsupp.PS_SUPPLYCOST", "$partsupp.PS_AVAILQTY"]
            }
        }
    }},
    {"$match": {
        "VALUE": {
            "$gt": {
                "$multiply": ["$VALUE", 0.0001000000]
            }
        }
    }},
    {"$sort": SON([("VALUE", -1)])}
]

output = list(db.nation.aggregate(pipeline))

df = pd.DataFrame(output)
df.rename(columns={"_id": "PS_PARTKEY", "VALUE": "VALUE"}, inplace=True)
df.to_csv('query_output.csv', index=False)
