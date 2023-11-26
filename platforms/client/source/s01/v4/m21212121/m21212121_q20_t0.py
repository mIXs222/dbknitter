import pymongo
import pandas as pd
from datetime import datetime
import direct_redis
import csv

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
supplier_col = mongo_db["supplier"]
lineitem_col = mongo_db["lineitem"]

# Redis connection
redis = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Part names starting with 'forest'
pandas_part = pd.DataFrame(eval(redis.get('part')))
part_keys_forest = pandas_part[pandas_part['P_NAME'].str.startswith('forest')]['P_PARTKEY'].tolist()

# Get all part suppliers for parts with names starting with 'forest'
pandas_partsupp = pd.DataFrame(eval(redis.get('partsupp')))
pandas_partsupp = pandas_partsupp[pandas_partsupp['PS_PARTKEY'].isin(part_keys_forest)]

# Calculate line quantity from MongoDB
pipeline = [
    {"$match": {
        "L_PARTKEY": {"$in": part_keys_forest},
        "L_SHIPDATE": {"$gte": datetime(1994, 1, 1), "$lt": datetime(1995, 1, 1)}
    }},
    {"$group": {
        "_id": {
            "L_PARTKEY": "$L_PARTKEY",
            "L_SUPPKEY": "$L_SUPPKEY"
        },
        "total_quantity": {"$sum": "$L_QUANTITY"}
    }}
]

line_quantities = {}
for doc in lineitem_col.aggregate(pipeline):
    key = (doc["_id"]["L_PARTKEY"], doc["_id"]["L_SUPPKEY"])
    line_quantities[key] = 0.5 * doc['total_quantity']

# Filter partsupp rows where available quantity is greater than half the summed lineitem quantity
supp_keys_filtered = []
for index, row in pandas_partsupp.iterrows():
    key = (row['PS_PARTKEY'], row['PS_SUPPKEY'])
    if key in line_quantities and row['PS_AVAILQTY'] > line_quantities[key]:
        supp_keys_filtered.append(row['PS_SUPPKEY'])

# Fetch suppliers that are from 'CANADA' using Mongodb and Redis
nation_data = pd.DataFrame(eval(redis.get('nation')))
nation_data_canada = nation_data[nation_data['N_NAME'] == 'CANADA']

suppliers_data = pd.DataFrame(list(supplier_col.find(
    {"S_SUPPKEY": {"$in": supp_keys_filtered}, "S_NATIONKEY": {"$in": nation_data_canada['N_NATIONKEY'].tolist()}}
)))

# Select the required columns
result_data = suppliers_data[['S_NAME', 'S_ADDRESS']]

# Write results to file
result_data.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
