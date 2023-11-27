# query.py
import pymongo
import csv
import datetime

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Prepare the queries
seven_years_ago = datetime.datetime.now() - datetime.timedelta(days=(7 * 365))
required_country_codes = ['20', '40', '22', '30', '39', '42', '21']

# Aggregation pipeline for MongoDB
pipeline = [
    {
        '$match': {
            'C_ACCTBAL': {'$gt': 0.0},
            'C_PHONE': {'$in': [f"{code}%" for code in required_country_codes]},
        }
    },
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'C_CUSTKEY',
            'foreignField': 'O_CUSTKEY',
            'as': 'orders'
        }
    },
    {
        '$match': {
            'orders': {'$not': {'$elemMatch': {'O_ORDERDATE': {'$gte': seven_years_ago}}}},
        }
    },
    {
        '$project': {
            'country_code': {'$substrBytes': ['$C_PHONE', 0, 2]},
            'C_CUSTKEY': 1,
            'C_ACCTBAL': 1
        }
    },
    {
        '$group': {
            '_id': '$country_code',
            'customer_count': {'$sum': 1},
            'total_balance': {'$sum': '$C_ACCTBAL'}
        }
    }
]

# Run the aggregation
result = db['customer'].aggregate(pipeline)

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['country_code', 'customer_count', 'total_balance']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for item in result:
        writer.writerow({
            'country_code': item['_id'],
            'customer_count': item['customer_count'],
            'total_balance': item['total_balance']
        })
