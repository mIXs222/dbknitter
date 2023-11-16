# query.py
import pymongo
import csv
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client.tpch

# Retrieve data from MongoDB collections
nation_data = {doc['N_NATIONKEY']: doc for doc in db.nation.find()}
customer_data = list(db.customer.find())
orders_data = list(db.orders.find({
    'O_ORDERDATE': {'$gte': datetime(1993, 10, 1), '$lt': datetime(1994, 1, 1)}
}))
lineitem_data = list(db.lineitem.find({'L_RETURNFLAG': 'R'}))

# Filter orders and lineitems
orders_dict = {doc['O_ORDERKEY']: doc for doc in orders_data}

# Join and calculate data
result = []
for customer in customer_data:
    customer_nation = nation_data.get(customer['C_NATIONKEY'])
    if customer_nation:
        for order in orders_data:
            if order['O_CUSTKEY'] == customer['C_CUSTKEY']:
                for lineitem in lineitem_data:
                    if lineitem['L_ORDERKEY'] == order['O_ORDERKEY']:
                        revenue = round(float(lineitem['L_EXTENDEDPRICE']) * (1 - float(lineitem['L_DISCOUNT'])), 2)
                        result.append({
                            'C_CUSTKEY': customer['C_CUSTKEY'],
                            'C_NAME': customer['C_NAME'],
                            'REVENUE': revenue,
                            'C_ACCTBAL': customer['C_ACCTBAL'],
                            'N_NAME': customer_nation['N_NAME'],
                            'C_ADDRESS': customer['C_ADDRESS'],
                            'C_PHONE': customer['C_PHONE'],
                            'C_COMMENT': customer['C_COMMENT'],
                        })

# Sort results
result.sort(key=lambda x: (-x['REVENUE'], x['C_CUSTKEY'], x['C_NAME'], x['C_ACCTBAL']))

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=result[0].keys())
    writer.writeheader()
    writer.writerows(result)
created_time, 1700094298
completion_tokens, 462
prompt_tokens, 584
