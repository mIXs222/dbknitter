from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the query time range
start_date = datetime.strptime('1993-10-01', '%Y-%m-%d')
end_date = datetime.strptime('1994-01-01', '%Y-%m-%d')

# Perform the query
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gte': start_date, '$lt': end_date},
            'L_RETURNFLAG': 'R'
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
            'from': 'nation',
            'localField': 'customer.C_NATIONKEY',
            'foreignField': 'N_NATIONKEY',
            'as': 'nation'
        }
    },
    {
        '$unwind': '$nation'
    },
    {
        '$group': {
            '_id': {
                'C_CUSTKEY': '$customer.C_CUSTKEY',
                'C_NAME': '$customer.C_NAME',
                'C_ACCTBAL': '$customer.C_ACCTBAL',
                'C_ADDRESS': '$customer.C_ADDRESS',
                'C_PHONE': '$customer.C_PHONE',
                'N_NAME': '$nation.N_NAME',
                'C_COMMENT': '$customer.C_COMMENT',
            },
            'REVENUE_LOST': {
                '$sum': {
                    '$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]
                }
            }
        }
    },
    {
        '$project': {
            'C_CUSTKEY': '$_id.C_CUSTKEY',
            'C_NAME': '$_id.C_NAME',
            'REVENUE_LOST': '$REVENUE_LOST',
            'C_ACCTBAL': '$_id.C_ACCTBAL',
            'C_ADDRESS': '$_id.C_ADDRESS',
            'C_PHONE': '$_id.C_PHONE',
            'N_NAME': '$_id.N_NAME',
            'C_COMMENT': '$_id.C_COMMENT',
            '_id': 0
        }
    },
    {
        '$sort': {
            'REVENUE_LOST': 1,
            'C_CUSTKEY': 1,
            'C_NAME': 1,
            'C_ACCTBAL': -1
        }
    }
]

result = db['lineitem'].aggregate(pipeline)

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    csv_writer = csv.DictWriter(file, fieldnames=['C_CUSTKEY', 'C_NAME', 'REVENUE_LOST', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'])
    csv_writer.writeheader()
    for data in result:
        csv_writer.writerow(data)
