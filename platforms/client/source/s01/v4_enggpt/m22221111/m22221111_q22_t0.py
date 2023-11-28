from pymongo import MongoClient
from statistics import mean

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
customers_coll = db['customer']
orders_coll = db['orders']

# Querying customers excluding those who have placed orders
customers_with_no_orders = customers_coll.find({
    'C_CUSTKEY': {
        '$nin': orders_coll.distinct('O_CUSTKEY')
    }
})

# Extracting the required country codes for filtration
country_codes = ['20', '40', '22', '30', '39', '42', '21']

# Computing the average account balance for customers with positive balances in the specified country codes
average_balances = {
    cc: mean([
        cust['C_ACCTBAL']
        for cust in customers_coll.find({'C_PHONE': {'$regex': f'^{cc}'}})
        if cust['C_ACCTBAL'] > 0
    ])
    for cc in country_codes
}
average_balance = mean(average_balances.values())

# Filtering customers based on criteria
filtered_customers = [
    (cust['C_PHONE'][:2], cust['C_ACCTBAL'])
    for cust in customers_with_no_orders
    if (
        cust['C_PHONE'][:2] in country_codes and
        cust['C_ACCTBAL'] > average_balance
    )
]

# Aggregating data based on country codes
results = {}

for cntrycode, acctbal in filtered_customers:
    if cntrycode not in results:
        results[cntrycode] = {
            'NUMCUST': 0,
            'TOTACCTBAL': 0.0
        }
    results[cntrycode]['NUMCUST'] += 1
    results[cntrycode]['TOTACCTBAL'] += acctbal

# Writing to CSV
import csv

with open('query_output.csv', mode='w') as csv_file:
    fieldnames = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for cntry_code, values in sorted(results.items()):
        writer.writerow({
            'CNTRYCODE': cntry_code,
            'NUMCUST': values['NUMCUST'],
            'TOTACCTBAL': values['TOTACCTBAL'],
        })
