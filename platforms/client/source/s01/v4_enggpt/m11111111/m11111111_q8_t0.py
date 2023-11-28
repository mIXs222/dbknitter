from pymongo import MongoClient
import csv
import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Aggregation Pipeline for MongoDB
pipeline = [
    {'$match': {
        'P_TYPE': 'SMALL PLATED COPPER',
    }},
    {'$lookup': {
        'from': 'lineitem',
        'localField': 'P_PARTKEY',
        'foreignField': 'L_PARTKEY',
        'as': 'lineitems'
    }},
    {'$unwind': '$lineitems'},
    {'$lookup': {
        'from': 'orders',
        'localField': 'lineitems.L_ORDERKEY',
        'foreignField': 'O_ORDERKEY',
        'as': 'orders'
    }},
    {'$unwind': '$orders'},
    {'$lookup': {
        'from': 'customer',
        'localField': 'orders.O_CUSTKEY',
        'foreignField': 'C_CUSTKEY',
        'as': 'customers'
    }},
    {'$unwind': '$customers'},
    {'$lookup': {
        'from': 'nation',
        'localField': 'customers.C_NATIONKEY',
        'foreignField': 'N_NATIONKEY',
        'as': 'nations'
    }},
    {'$unwind': '$nations'},
    {'$lookup': {
        'from': 'region',
        'localField': 'nations.N_REGIONKEY',
        'foreignField': 'R_REGIONKEY',
        'as': 'regions'
    }},
    {'$unwind': '$regions'},
    {'$match': {
        'regions.R_NAME': 'ASIA',
        'nations.N_NAME': 'INDIA',
        'orders.O_ORDERDATE': {
            '$gte': datetime.datetime(1995, 1, 1),
            '$lt': datetime.datetime(1997, 1, 1)
        }
    }},
    {'$project': {
        'year': {'$year': '$orders.O_ORDERDATE'},
        'volume': {
            '$multiply': [
                '$lineitems.L_EXTENDEDPRICE',
                {'$subtract': [1, '$lineitems.L_DISCOUNT']}
            ]
        }
    }},
    {'$group': {
        '_id': {'year': '$year'},
        'total_volume': {'$sum': '$volume'},
        'total_all_volume': {'$sum': 1}
    }},
    {'$sort': {'_id.year': 1}}
]

# Execute the aggregation pipeline
result = list(db['part'].aggregate(pipeline))

# Calculate market share based on INDIA's volume divided by total volume for each year
for data in result:
    data['market_share'] = data['total_volume'] / data['total_all_volume']

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['year', 'market_share']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in result:
        writer.writerow({'year': data['_id']['year'], 'market_share': data['market_share']})

print("Market share analysis completed. Results written to query_output.csv")
