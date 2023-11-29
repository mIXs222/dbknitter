# sales_opportunity_query.py

from pymongo import MongoClient
import csv
from datetime import datetime, timedelta

# Connect to the MongoDB server
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the list of valid country codes
valid_cntrycodes = ['20', '40', '22', '30', '39', '42', '21']

# Get today's date to calculate orders from 7 years ago
seven_years_ago = datetime.now() - timedelta(days=365*7)

# Find the average account balance of people with account balance > 0.00 in those countries
avg_balance_pipeline = [
    {
        '$match': {
            'C_PHONE': {'$regex': '^(' + '|'.join(valid_cntrycodes) + ')'},
            'C_ACCTBAL': {'$gt': 0}
        }
    },
    {
        '$group': {
            '_id': {'CNTRYCODE': {'$substr': ['$C_PHONE', 0, 2]}},
            'AVG_BALANCE': {'$avg': '$C_ACCTBAL'}
        }
    }
]

avg_balances = {doc['_id']['CNTRYCODE']: doc['AVG_BALANCE'] for doc in db.customer.aggregate(avg_balance_pipeline)}

# Retrieve customers that have not placed orders for 7 years
customers_no_orders = db.customer.find(
    {
        'C_PHONE': {'$regex': '^(' + '|'.join(valid_cntrycodes) + ')'},
        'C_ACCTBAL': {'$gt': 0}
    }
)

# Prepare the data structure for the results
results = {}

for customer in customers_no_orders:
    cntrycode = customer['C_PHONE'][:2]
    
    # Check if the customer has orders within the past 7 years
    recent_order = db.orders.find_one(
        {
            'O_CUSTKEY': customer['C_CUSTKEY'],
            'O_ORDERDATE': {'$gte': seven_years_ago}
        }
    )

    # Check if customer's balance is greater than the average in his/her country
    if not recent_order and customer['C_ACCTBAL'] > avg_balances[cntrycode]:
        if cntrycode not in results:
            results[cntrycode] = {'NUM_CUSTOMERS': 0, 'TOTAL_BALANCE': 0}
        
        results[cntrycode]['NUM_CUSTOMERS'] += 1
        results[cntrycode]['TOTAL_BALANCE'] += customer['C_ACCTBAL']

# Write the results to file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['CNTRYCODE', 'NUM_CUSTOMERS', 'TOTAL_BALANCE'])
    
    for cntrycode, data in sorted(results.items()):
        writer.writerow([cntrycode, data['NUM_CUSTOMERS'], data['TOTAL_BALANCE']])
