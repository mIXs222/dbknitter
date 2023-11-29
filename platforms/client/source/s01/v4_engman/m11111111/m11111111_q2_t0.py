# query.py
import pymongo
import csv

# Connect to the MongoDB
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# MongoDB collections
nation_coll = db['nation']
region_coll = db['region']
part_coll = db['part']
supplier_coll = db['supplier']
partsupp_coll = db['partsupp']

# Perform the query
europe_region_key = region_coll.find_one({'R_NAME': 'EUROPE'}, {'R_REGIONKEY': 1})
brass_parts = part_coll.find(
    {'P_TYPE': 'BRASS', 'P_SIZE': 15}, 
    {'P_PARTKEY': 1, 'P_MFGR': 1}
)
partkeys_brass_parts = [part['P_PARTKEY'] for part in brass_parts]

# Aggregate to find minimum costs with the other details needed
pipeline = [
    {'$match': {'PS_PARTKEY': {'$in': partkeys_brass_parts}}},
    {'$lookup': {
        'from': 'supplier',
        'localField': 'PS_SUPPKEY',
        'foreignField': 'S_SUPPKEY',
        'as': 'suppliers'
    }},
    {'$unwind': '$suppliers'},
    {'$lookup': {
        'from': 'nation',
        'localField': 'suppliers.S_NATIONKEY',
        'foreignField': 'N_NATIONKEY',
        'as': 'nation'
    }},
    {'$unwind': '$nation'},
    {'$lookup': {
        'from': 'part',
        'localField': 'PS_PARTKEY',
        'foreignField': 'P_PARTKEY',
        'as': 'part'
    }},
    {'$unwind': '$part'},
    {'$match': {'nation.N_REGIONKEY': europe_region_key['R_REGIONKEY']}},
    {'$sort': {'PS_SUPPLYCOST': 1, 'suppliers.S_ACCTBAL': -1, 'nation.N_NAME': 1, 'suppliers.S_NAME': 1, 'PS_PARTKEY': 1}},
    {'$group': {
        '_id': '$PS_PARTKEY',
        'suppliers': {'$first': '$suppliers'},
        'part': {'$first': '$part'},
        'PS_SUPPLYCOST': {'$first': '$PS_SUPPLYCOST'},
        'nation': {'$first': '$nation'}
    }}
]

results = partsupp_coll.aggregate(pipeline)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Define columns header
    fields = ['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']
    csvwriter.writerow(fields)

    # Write result rows to the CSV file
    for document in results:
        row = [
            document['nation']['N_NAME'],
            document['part']['P_MFGR'],
            document['_id'],
            document['suppliers']['S_ACCTBAL'],
            document['suppliers']['S_ADDRESS'],
            document['suppliers']['S_COMMENT'],
            document['suppliers']['S_NAME'],
            document['suppliers']['S_PHONE'],
        ]
        csvwriter.writerow(row)
