import pymongo
import pandas as pd
from pymongo import MongoClient

# connect MongoDB
client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# define tables
table_names = ["supplier", "partsupp", "part", "lineitem", "nation"]
tables = {name: db[name] for name in table_names}

# Get PartKeys from Part table where part's name starts with 'forest'
partkeys = [p['P_PARTKEY'] for p in tables['part'].find({"P_NAME": {"$regex": "^forest"}})]

# Get PartSuppKeys from PartSupp table where PartKey is in 'partkeys' list and available quantity > half of the shipped quantity in the time-period
suppKeys = []
for item in tables['partsupp'].find():
    if item['PS_PARTKEY'] in partkeys:
        total_quantity = tables['lineitem'].aggregate([
            {"$match": {
                "L_PARTKEY": item['PS_PARTKEY'], 
                "L_SUPPKEY": item['PS_SUPPKEY'],
                "L_SHIPDATE": {"$gte": "1994-01-01", "$lt": "1995-01-01"}
            }},
            {"$group": {"_id": None, "sum": {"$sum": '$L_QUANTITY'}}}
        ])
        if list(total_quantity)[0]["sum"] > 0.5 * item['PS_AVAILQTY']:
            suppKeys.append(item['PS_SUPPKEY'])

# Get nation_key from Nation table where nation's name is 'CANADA'
nation_key = tables['nation'].find_one({"N_NAME": 'CANADA'})['N_NATIONKEY']

# Fetch name and address of suppliers where SUPPKEY is in 'suppKeys' list and nation_key matches with the 'nation_key'
result = tables['supplier'].find({"S_SUPPKEY": {"$in": suppKeys}, "S_NATIONKEY": nation_key}, {"S_NAME": 1, "S_ADDRESS": 1})

# Convert result to DataFrame and output to CSV
result_df = pd.DataFrame(list(result))
result_df.to_csv('query_output.csv', index=False)
