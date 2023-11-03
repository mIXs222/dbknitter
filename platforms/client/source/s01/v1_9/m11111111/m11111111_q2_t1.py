from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://mongodb:27017/")
db = client['tpch']

pipeline = [
    {
        # First join collections supplier, nation, and partsupp
        '$lookup': {
            'from': 'supplier',
            'localField': 'S_SUPPKEY',
            'foreignField': 'PS_SUPPKEY',
            'as': 'supplier_partsupp'
        }
    }, {
        '$unwind': '$supplier_partsupp'
    }, {
        '$lookup': {
            'from': 'nation',
            'localField': 'supplier_partsupp.S_NATIONKEY',
            'foreignField': 'N_NATIONKEY',
            'as': 'nation_supplier_partsupp'
        }
    }, {
        '$unwind': '$nation_supplier_partsupp'
    }, {
        
        # Second join with part and region collections
        '$lookup': {
            'from': 'part',
            'localField': 'P_PARTKEY',
            'foreignField': 'PS_PARTKEY',
            'as': 'part'
        }
    }, {
        '$unwind': '$part'
    }, {
        '$lookup': {
            'from': 'region',
            'localField': 'nation_supplier_partsupp.N_REGIONKEY',
            'foreignField': 'R_REGIONKEY',
            'as': 'region'
        }
    }, {
        '$unwind': '$region'
    }, {
        # Apply WHERE conditions
        '$match': {
            'part.P_SIZE': 15,
            'part.P_TYPE': { '$regex': 'BRASS' },
            'region.R_NAME': 'EUROPE',
            'supplier_partsupp.PS_SUPPLYCOST': {
                '$eq': db.partsupp.aggregate([
                    { 
                        '$group': {
                            '_id': None, 
                            'min': { '$min': "$PS_SUPPLYCOST" }
                        }
                    }
                ]).next()['min']
            }
        }
    }, {
        # Select necessary fields
        '$project': {
            '_id': 0,
            'S_ACCTBAL': '$supplier_partsupp.S_ACCTBAL',
            'S_NAME': '$supplier_partsupp.S_NAME',
            'N_NAME': '$nation_supplier_partsupp.N_NAME',
            'P_PARTKEY': '$part.P_PARTKEY',
            'P_MFGR': '$part.P_MFGR',
            'S_ADDRESS': '$supplier_partsupp.S_ADDRESS',
            'S_PHONE': '$supplier_partsupp.S_PHONE',
            'S_COMMENT': '$supplier_partsupp.S_COMMENT'
        }
    }, {
        # Apply ORDER BY conditions
        '$sort': { 'S_ACCTBAL': -1, 'N_NAME': 1, 'S_NAME': 1, 'P_PARTKEY': 1 }
    }
]

result = list(db.partsupp.aggregate(pipeline))

# Write to CSV file
df = pd.DataFrame(result)
df.to_csv('query_output.csv', index=False)
