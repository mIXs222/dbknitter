from pymongo import MongoClient
import csv
from datetime import datetime, timedelta

# Connection details
host = 'mongodb'
port = 27017
dbname = 'tpch'

# Establish a connection to the MongoDB server
client = MongoClient(host, port)
db = client[dbname]

# Retrieve customers and orders into memory
customers = list(db.customer.find({}))
orders = list(db.orders.find({}))

# Filter out customers by the specified country codes
country_codes = ['20', '40', '22', '30', '39', '42', '21']
filtered_customers = [c for c in customers if c['C_PHONE'][:2] in country_codes]

# Get the current date and calculate the date 7 years ago
seven_years_ago = datetime.now() - timedelta(days=7*365)

# Find all customers who have not ordered for 7 years
customers_without_orders = [
    c for c in filtered_customers if not any(
        o['O_CUSTKEY'] == c['C_CUSTKEY'] and datetime.strptime(o['O_ORDERDATE'], "%Y-%m-%d") > seven_years_ago
        for o in orders
    )
]

# Calculate the average account balance for customers with positive balance
average_balance = (
    sum(c['C_ACCTBAL'] for c in filtered_customers if c['C_ACCTBAL'] > 0) /
    sum(1 for c in filtered_customers if c['C_ACCTBAL'] > 0)
)

# Filter customers by the average account balance
target_customers = [
    c for c in customers_without_orders if c['C_ACCTBAL'] > average_balance
]

# Aggregate results by country code
results = {}
for c in target_customers:
    cntrycode = c['C_PHONE'][:2]
    if cntrycode not in results:
        results[cntrycode] = {'count': 0, 'total_balance': 0}
    results[cntrycode]['count'] += 1
    results[cntrycode]['total_balance'] += c['C_ACCTBAL']

# Sort results by CNTRYCODE ascending
sorted_results = sorted(results.items())

# Write results to query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['CNTRYCODE', 'CUSTOMER_COUNT', 'TOTAL_BALANCE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for cntrycode, data in sorted_results:
        writer.writerow({
            'CNTRYCODE': cntrycode,
            'CUSTOMER_COUNT': data['count'],
            'TOTAL_BALANCE': data['total_balance']
        })
