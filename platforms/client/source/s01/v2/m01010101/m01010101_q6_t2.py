from pymongo import MongoClient
import pandas as pd

# Connection setup
client = MongoClient("mongodb://localhost:27017/")
db = client["tpch"]
collection = db["lineitem"]

# Perform the query
pipeline = [
    {'$match': {
        'L_SHIPDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'},
        'L_DISCOUNT': {'$gte': .06 - 0.01, '$lte': .06 + 0.01},
        'L_QUANTITY': {'$lt': 24}}},
    {'$group': {
        '_id': None,
        'REVENUE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', '$L_DISCOUNT']}}}}
]
result = list(collection.aggregate(pipeline))

# Save the result to CSV
df = pd.DataFrame(result)
df.to_csv("query_output.csv", index=False)

