# query.py
from pymongo import MongoClient
import csv
from decimal import Decimal
import os

# Function to perform the query and return the results
def query_market_share(client):
    pipeline = [
        {
            '$lookup': {
                'from': 'supplier',
                'localField': 'L_SUPPKEY',
                'foreignField': 'S_SUPPKEY',
                'as': 'supplier'
            }
        },
        {
            '$unwind': '$supplier'
        },
        {
            '$lookup': {
                'from': 'nation',
                'localField': 'supplier.S_NATIONKEY',
                'foreignField': 'N_NATIONKEY',
                'as': 'nation'
            }
        },
        {
            '$unwind': '$nation'
        },
        {
            '$lookup': {
                'from': 'region',
                'localField': 'nation.N_REGIONKEY',
                'foreignField': 'R_REGIONKEY',
                'as': 'region'
            }
        },
        {
            '$unwind': '$region'
        },
        {
            '$lookup': {
                'from': 'part',
                'localField': 'L_PARTKEY',
                'foreignField': 'P_PARTKEY',
                'as': 'part'
            }
        },
        {
            '$unwind': '$part'
        },
        {
            '$match': {
                'nation.N_NAME': 'INDIA',
                'region.R_NAME': 'ASIA',
                'part.P_TYPE': 'SMALL PLATED COPPER',
                '$expr': {
                    '$or': [
                        {'$eq': [{'$year': '$L_SHIPDATE'}, 1995]},
                        {'$eq': [{'$year': '$L_SHIPDATE'}, 1996]}
                    ]
                }
            }
        },
        {
            '$project': {
                '_id': 0,
                'order_year': {'$year': '$L_SHIPDATE'},
                'value': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE',
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            }
        },
        {
            '$group': {
                '_id': '$order_year',
                'total_value': {'$sum': '$value'}
            }
        },
        {
            '$sort': {'_id': 1}
        }
    ]

    result = list(client.tpch.lineitem.aggregate(pipeline))
    total_values = {doc['_id']: doc['total_value'] for doc in result}

    return [
        {'order_year': 1995, 'market_share': total_values.get(1995, Decimal('0.0'))},
        {'order_year': 1996, 'market_share': total_values.get(1996, Decimal('0.0'))}
    ]

# Connection to MongoDB
client = MongoClient('mongodb', 27017)
results = query_market_share(client)

# Write results to CSV
output_file = 'query_output.csv'
with open(output_file, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['order_year', 'market_share'])
    writer.writeheader()
    for record in results:
        writer.writerow(record)

# Close MongoDB connection
client.close()
