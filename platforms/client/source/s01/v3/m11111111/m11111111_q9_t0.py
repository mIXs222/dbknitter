import csv
import pymongo
from pymongo import MongoClient
from datetime import datetime

# MongoDB setup
client = MongoClient('mongodb://localhost:27017/')
db = client['tpch']

# Tables
tables = ["nation", "part", "supplier", "partsupp", "orders", "lineitem"]

# Join the tables
records = []
for table in tables:
    if table in db.list_collection_names():
        records.extend(list(db[table].find()))

# Combine the data based on the conditions
result = []
for record in records:
    if 'S_SUPPKEY' in record and 'L_SUPPKEY' in record and 'PS_SUPPKEY' in record and 'PS_PARTKEY' in record and 'P_PARTKEY' in record and 'O_ORDERKEY' in record and 'S_NATIONKEY' in record and 'N_NATIONKEY' in record and 'P_NAME' in record and 'dim' in record['P_NAME']:
        amount = record['L_EXTENDEDPRICE'] * (1 - record['L_DISCOUNT']) - record['PS_SUPPLYCOST'] * record['L_QUANTITY']
        result.append({
            'NATION': record['N_NAME'],
            'O_YEAR': datetime.strptime(record['O_ORDERDATE'], '%Y-%m-%d').year,
            'SUM_PROFIT': amount
        })

# Write the result to the csv file
keys = result[0].keys()
with open('query_output.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(result)
