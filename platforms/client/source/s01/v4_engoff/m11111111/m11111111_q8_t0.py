# market_share.py
import pymongo
import csv
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
db = client.tpch

# Find the nation and region keys for 'INDIA' and 'ASIA'
india_nation = db.nation.find_one({'N_NAME': 'INDIA'})
asia_region = db.region.find_one({'R_NAME': 'ASIA'})

if not india_nation or not asia_region:
    raise ValueError("The required 'INDIA' nation or 'ASIA' region could not be found in the database")

india_nation_key = india_nation['N_NATIONKEY']
asia_region_key = asia_region['R_REGIONKEY']

# MongoDB Aggregation pipeline
pipeline = [
    {
        '$match': {
            'S_NATIONKEY': india_nation_key
        }
    }, {
        '$lookup': {
            'from': 'part', 
            'localField': 'S_SUPPKEY', 
            'foreignField': 'P_PARTKEY', 
            'as': 'part'
        }
    }, {
        '$unwind': {
            'path': '$part', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$match': {
            'part.P_TYPE': 'SMALL PLATED COPPER'
        }
    }, {
        '$lookup': {
            'from': 'lineitem', 
            'localField': 'S_SUPPKEY', 
            'foreignField': 'L_SUPPKEY', 
            'as': 'lineitem'
        }
    }, {
        '$unwind': {
            'path': '$lineitem', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$match': {
            '$expr': {
                '$and': [
                    {'$eq': ['$lineitem.L_PARTKEY', '$part.P_PARTKEY']},
                    {'$in': [{'$year': '$lineitem.L_SHIPDATE'}, [1995, 1996]]}
                ]
            }
        }
    }, {
        '$group': {
            '_id': {'year': {'$year': '$lineitem.L_SHIPDATE'}},
            'revenue': {
                '$sum': {
                    '$multiply': [
                        '$lineitem.L_EXTENDEDPRICE',
                        {'$subtract': [1, '$lineitem.L_DISCOUNT']}
                    ]
                }
            }
        }
    }
]

result = db.supplier.aggregate(pipeline)

# Calculate total revenue for Asia in 1995 and 1996 to determine market share
total_revenue = {1995: 0, 1996: 0}

# Process the results
market_shares = {}
for doc in result:
    year = doc['_id']['year']
    revenue = doc['revenue']
    total_revenue[year] += revenue
    market_shares[year] = revenue

# Write to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Year', 'Market Share'])
    for year in sorted(market_shares.keys()):
        writer.writerow([year, market_shares[year]])

# Close the MongoDB connection
client.close()
