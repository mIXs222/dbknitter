# file: execute_query.py

from pymongo import MongoClient
import csv

# MongoDB connection
client = MongoClient('mongodb', 27017)
db = client['tpch']
customers = db['customer']
orders = db['orders']

# Get the average account balance from customers with positive balance
# and phone number from specified countries
countries = ('20', '40', '22', '30', '39', '42', '21')
query_avg_balance = {
    'C_ACCTBAL': {'$gt': 0},
    'C_PHONE': {'$regex': f'^({"|".join(countries)})'}
}
avg_balance = db.customer.aggregate([
    {'$match': query_avg_balance},
    {'$group': {'_id': None, 'average_balance': {'$avg': '$C_ACCTBAL'}}}
])
avg_acct_bal = next(avg_balance)['average_balance']

# Build the customer query with conditions
cust_query = {
    'C_ACCTBAL': {'$gt': avg_acct_bal},
    'C_PHONE': {'$regex': f'^({"|".join(countries)})'}
}

# Prepare the pipeline to execute the aggregation on MongoDB
pipeline = [
    {'$match': cust_query},
    {'$lookup': {
        'from': 'orders',
        'localField': 'C_CUSTKEY',
        'foreignField': 'O_CUSTKEY',
        'as': 'customer_orders'
    }},
    {'$match': {'customer_orders': {'$eq': []}}},
    {'$project': {
        'CNTRYCODE': {'$substr': ['$C_PHONE', 0, 2]},
        'C_ACCTBAL': 1
    }}
]

# Execute the MongoDB aggregation pipeline
cust_results = customers.aggregate(pipeline)

# Group by country code in Python and write to CSV
results = {}
for customer in cust_results:
    cntrycode = customer['CNTRYCODE']
    data = results.get(cntrycode, {'NUMCUST': 0, 'TOTACCTBAL': 0})
    data['NUMCUST'] += 1
    data['TOTACCTBAL'] += customer['C_ACCTBAL']
    results[cntrycode] = data

# Sort results by country code
sorted_results = sorted(results.items(), key=lambda x: x[0])

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL'])
    for cntrycode, data in sorted_results:
        writer.writerow([cntrycode, data['NUMCUST'], data['TOTACCTBAL']])
