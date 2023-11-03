from pymongo import MongoClient
import pandas as pd

# connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# prepare the pipeline to execute the equivalent query in MongoDB

pipeline_germany_inferior = [
    {"$lookup":{"from":"supplier","localField":"PS_SUPPKEY","foreignField":"S_SUPPKEY","as":"supplier_nation"}},
    {"$unwind":"$supplier_nation"},
    {"$lookup":{"from":"nation","localField":"supplier_nation.S_NATIONKEY","foreignField":"N_NATIONKEY","as":"nation"}},
    {"$unwind":"$nation"},
    {"$match":{"nation.N_NAME":"GERMANY"}},
    {"$group":{"_id": None, "total": {"$sum": {"$multiply":["$PS_SUPPLYCOST", "$PS_AVAILQTY"]}}}},
    {"$project":{"_id":0,"total":1}}
]

german_inferior_value = list(db.partsupp.aggregate(pipeline_germany_inferior))[0]['total']*0.0001000000

pipeline_germany_superior = [
    {"$lookup":{"from":"supplier","localField":"PS_SUPPKEY","foreignField":"S_SUPPKEY","as":"supplier_nation"}},
    {"$unwind":"$supplier_nation"},
    {"$lookup":{"from":"nation","localField":"supplier_nation.S_NATIONKEY","foreignField":"N_NATIONKEY","as":"nation"}},
    {"$unwind":"$nation"},
    {"$match":{"nation.N_NAME":"GERMANY"}},
    {"$group":{"_id": "$PS_PARTKEY", "value": {"$sum": {"$multiply":["$PS_SUPPLYCOST", "$PS_AVAILQTY"]}}}},
    {"$match":{"value":{"$gt":german_inferior_value}}},
    {"$sort":{"value":-1}}
]

# execute the pipeline
result = list(db.partsupp.aggregate(pipeline_germany_superior))

# convert to pandas dataframe
df = pd.DataFrame(result)

# write to csv
df.to_csv('query_output.csv')
