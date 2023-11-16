import pymongo
import csv
from datetime import datetime

# Establish connection with MongoDB
client = pymongo.MongoClient("mongodb", 27017)

# Access the 'tpch' database
db = client.tpch

# Function to convert string to date
def str_to_date(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d')

# Function to fetch and process the data
def process_data():
    pipeline = [
        {
            '$lookup': {
                'from': 'lineitem',
                'localField': 'S_SUPPKEY',
                'foreignField': 'L_SUPPKEY',
                'as': 'lineitems'
            }
        },
        {'$unwind': '$lineitems'},
        {
            '$lookup': {
                'from': 'orders',
                'localField': 'lineitems.L_ORDERKEY',
                'foreignField': 'O_ORDERKEY',
                'as': 'orders'
            }
        },
        {'$unwind': '$orders'},
        {
            '$lookup': {
                'from': 'customer',
                'localField': 'orders.O_CUSTKEY',
                'foreignField': 'C_CUSTKEY',
                'as': 'customers'
            }
        },
        {'$unwind': '$customers'},
        {
            '$lookup': {
                'from': 'nation',
                'localField': 'S_NATIONKEY',
                'foreignField': 'N_NATIONKEY',
                'as': 'supp_nation'
            }
        },
        {'$unwind': '$supp_nation'},
        {
            '$lookup': {
                'from': 'nation',
                'localField': 'customers.C_NATIONKEY',
                'foreignField': 'N_NATIONKEY',
                'as': 'cust_nation'
            }
        },
        {'$unwind': '$cust_nation'},
        {'$match': {
            'supp_nation.N_NAME': {'$in': ['JAPAN', 'INDIA']},
            'cust_nation.N_NAME': {'$in': ['JAPAN', 'INDIA']},
            'supp_nation.N_NAME': {'$ne': '$cust_nation.N_NAME'},
            'lineitems.L_SHIPDATE': {'$gte': str_to_date('1995-01-01'), '$lte': str_to_date('1996-12-31')}
        }},
        {'$project': {
            'SUPP_NATION': '$supp_nation.N_NAME',
            'CUST_NATION': '$cust_nation.N_NAME',
            'L_YEAR': {'$year': '$lineitems.L_SHIPDATE'},
            'VOLUME': {'$multiply': ['$lineitems.L_EXTENDEDPRICE', {'$subtract': [1, '$lineitems.L_DISCOUNT']}]}
        }},
        {'$group': {
            '_id': {
                'SUPP_NATION': '$SUPP_NATION',
                'CUST_NATION': '$CUST_NATION',
                'L_YEAR': '$L_YEAR'
            },
            'REVENUE': {'$sum': '$VOLUME'}
        }},
        {'$sort': {
            '_id.SUPP_NATION': 1,
            '_id.CUST_NATION': 1,
            '_id.L_YEAR': 1
        }}
    ]

    return db.supplier.aggregate(pipeline)

# Write the query output to a CSV file
def write_to_csv(data):
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['SUPP_NATION', 'CUST_NATION', 'L_YEAR', 'REVENUE']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in data:
            writer.writerow({
                'SUPP_NATION': row['_id']['SUPP_NATION'],
                'CUST_NATION': row['_id']['CUST_NATION'],
                'L_YEAR': row['_id']['L_YEAR'],
                'REVENUE': row['REVENUE'],
            })

result = process_data()
write_to_csv(result)
