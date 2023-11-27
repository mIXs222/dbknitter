import csv
import pymongo
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['tpch']

pipeline = [
    {
        "$lookup": {
            "from": "part",
            "localField": "L_PARTKEY",
            "foreignField": "P_PARTKEY",
            "as": "part_docs"
        }
    },
    {
        "$match": {
            "part_docs.P_BRAND": "Brand#23",
            "part_docs.P_CONTAINER": "MED BAG",
            "L_QUANTITY": {"$lt" : 0.2}
        }
    },
    {
        "$group": {
            "_id": "$L_PARTKEY",
            "avg_yearly": {"$avg": "$L_EXTENDEDPRICE"}
        }
    }
]

results = db['lineitem'].aggregate(pipeline)
avg_yearly_results = [{'_id': result['_id'], 'avg_yearly': result['avg_yearly'] / 7.0} for result in results]

with open('query_output.csv', 'w') as f:
    writer = csv.DictWriter(f, fieldnames=['_id', 'avg_yearly'])
    writer.writeheader()
    writer.writerows(avg_yearly_results)
