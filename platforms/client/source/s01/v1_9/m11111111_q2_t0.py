from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

def get_min_supply_cost():
    pipeline = [{'$match': {'N_NAME': 'EUROPE'} },
                {'$lookup': {'from': 'supplier', 'localField': 'N_NATIONKEY', 'foreignField': 'S_NATIONKEY', 'as': 'supplier'} },
                {'$lookup': {'from': 'partsupp', 'localField': 'supplier.PS_SUPPKEY', 'foreignField': 'PS_SUPPKEY', 'as': 'partsupp'} },
                {'$unwind': '$partsupp'},
                {'$group': {'_id': None, 'minCost': { '$min': "$partsupp.PS_SUPPLYCOST" }}}]
    min_cost = list(db.nation.aggregate(pipeline))[0]["minCost"]
    return min_cost

pipeline = [
    {'$match': {'P_SIZE': 15, 'P_TYPE': {'$regex': 'BRASS'} }},
    {'$lookup': {'from': 'partsupp', 'localField': 'P_PARTKEY', 'foreignField': 'PS_PARTKEY', 'as': 'partsupp'} },
    {'$lookup': {'from': 'supplier', 'localField': 'partsupp.PS_SUPPKEY', 'foreignField': 'S_SUPPKEY', 'as': 'supplier'} },
    {'$lookup': {'from': 'nation', 'localField': 'supplier.S_NATIONKEY', 'foreignField': 'N_NATIONKEY', 'as': 'nation'} },
    {'$match': {'nation.N_NAME': 'EUROPE', 'partsupp.PS_SUPPLYCOST': get_min_supply_cost()} },
    {'$project': {'S_ACCTBAL': '$supplier.S_ACCTBAL', 'S_NAME': '$supplier.S_NAME', 'N_NAME': '$nation.N_NAME', 'P_PARTKEY': 1, 
                  'P_MFGR': 1, 'S_ADDRESS': '$supplier.S_ADDRESS', 'S_PHONE': '$supplier.S_PHONE', 'S_COMMENT': '$supplier.S_COMMENT'} },
    {'$sort': {'S_ACCTBAL': -1, 'N_NAME': 1, 'S_NAME': 1, 'P_PARTKEY': 1}}
]

cursor = db.part.aggregate(pipeline)

data = list(cursor)
df = pd.DataFrame(data)
df.to_csv('query_output.csv', index=False)
