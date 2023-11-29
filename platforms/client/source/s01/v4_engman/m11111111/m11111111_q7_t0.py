import pymongo
import csv
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client.tpch

# Define the query
start_date = datetime(1995, 1, 1)
end_date = datetime(1997, 1, 1)

pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
        }
    },
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'order'
        }
    },
    {
        '$unwind': '$order'
    },
    {
        '$lookup': {
            'from': 'customer',
            'localField': 'order.O_CUSTKEY',
            'foreignField': 'C_CUSTKEY',
            'as': 'customer'
        }
    },
    {
        '$unwind': '$customer'
    },
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
        '$match': {
            '$or': [
                {'customer.C_NATIONKEY': 'INDIA', 'supplier.S_NATIONKEY': 'JAPAN'},
                {'customer.C_NATIONKEY': 'JAPAN', 'supplier.S_NATIONKEY': 'INDIA'}
            ]
        }
    },
    {
        '$project': {
            'REVENUE': {
                '$multiply': [
                    '$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}
                ]
            },
            'CUST_NATION': '$customer.C_NATIONKEY',
            'SUPP_NATION': '$supplier.S_NATIONKEY',
            'L_YEAR': {'$year': '$L_SHIPDATE'}
        }
    },
    {
        '$group': {
            '_id': {
                'CUST_NATION': '$CUST_NATION',
                'SUPP_NATION': '$SUPP_NATION',
                'L_YEAR': '$L_YEAR'
            },
            'REVENUE': {'$sum': '$REVENUE'}
        }
    },
    {
        '$sort': {'_id.SUPP_NATION': 1, '_id.CUST_NATION': 1, '_id.L_YEAR': 1}
    },
    {
        '$project': {
            '_id': 0,
            'CUST_NATION': '$_id.CUST_NATION',
            'SUPP_NATION': '$_id.SUPP_NATION',
            'L_YEAR': '$_id.L_YEAR',
            'REVENUE': '$REVENUE'
        }
    }
]

# Execute the query
result = list(db.lineitem.aggregate(pipeline))

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION'])
    writer.writeheader()
    for data in result:
        writer.writerow(data)

print('The query results have been saved to query_output.csv')
