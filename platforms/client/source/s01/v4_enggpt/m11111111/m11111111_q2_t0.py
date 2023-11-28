from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Query data from MongoDB
pipeline = [
    {"$lookup": {
        "from": "nation",
        "localField": "S_NATIONKEY",
        "foreignField": "N_NATIONKEY",
        "as": "nation"
    }},
    {"$unwind": "$nation"},
    {"$lookup": {
        "from": "region",
        "localField": "nation.N_REGIONKEY",
        "foreignField": "R_REGIONKEY",
        "as": "region"
    }},
    {"$unwind": "$region"},
    {"$match": {
        "region.R_NAME": "EUROPE",
    }},
    {"$lookup": {
        "from": "partsupp",
        "localField": "S_SUPPKEY",
        "foreignField": "PS_SUPPKEY",
        "as": "partsupp"
    }},
    {"$unwind": "$partsupp"},
    {"$lookup": {
        "from": "part",
        "localField": "partsupp.PS_PARTKEY",
        "foreignField": "P_PARTKEY",
        "as": "part"
    }},
    {"$unwind": "$part"},
    {"$match": {
        "part.P_SIZE": 15,
        "part.P_TYPE": {"$regex": ".*BRASS.*"}
    }},
    {"$sort": {
        "S_ACCTBAL": -1,
        "nation.N_NAME": 1,
        "S_NAME": 1,
        "part.P_PARTKEY": 1,
    }},
    {"$project": {
        "S_ACCTBAL": 1,
        "S_NAME": 1,
        "S_ADDRESS": 1,
        "S_PHONE": 1,
        "S_COMMENT": 1,
        "P_PARTKEY": "$part.P_PARTKEY",
        "P_NAME": "$part.P_NAME",
        "P_MFGR": "$part.P_MFGR",
        "P_SIZE": "$part.P_SIZE",
        "N_NAME": "$nation.N_NAME",
        "_id": 0
    }}
]

result = db.supplier.aggregate(pipeline)

# Write to CSV
with open('query_output.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    # Write header
    writer.writerow(['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_SIZE', 'N_NAME'])
    
    # Write data
    for doc in result:
        writer.writerow([
            doc['S_ACCTBAL'],
            doc['S_NAME'],
            doc['S_ADDRESS'],
            doc['S_PHONE'],
            doc['S_COMMENT'],
            doc['P_PARTKEY'],
            doc['P_NAME'],
            doc['P_MFGR'],
            doc['P_SIZE'],
            doc['N_NAME']
        ])

print("Data has been written to query_output.csv")
