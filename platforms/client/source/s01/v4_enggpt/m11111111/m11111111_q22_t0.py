# python_code.py
from pymongo import MongoClient
import csv

# Connect to the MongoDB instance
client = MongoClient('mongodb', 27017)
db = client.tpch

# Query the customer and orders collections
customers = db.customer.find()
orders = db.orders.find()

# Filter customers with positive account balances and calculate average account balances per country code
positive_balance_customers = [c for c in customers if c['C_ACCTBAL'] > 0]
country_avg_balances = {}
for c in positive_balance_customers:
    cntrycode = c['C_PHONE'][:2]
    if cntrycode in country_avg_balances:
        country_avg_balances[cntrycode].append(c['C_ACCTBAL'])
    else:
        country_avg_balances[cntrycode] = [c['C_ACCTBAL']]

avg_balances = {k: sum(v) / len(v) for k, v in country_avg_balances.items()}

# Filter eligible country codes
eligible_country_codes = set(['20', '40', '22', '30', '39', '42', '21'])

# Retrieve customers based on the criteria
cust_sales = {}
for customer in positive_balance_customers:
    cntrycode = customer['C_PHONE'][:2]
    if cntrycode in eligible_country_codes and customer['C_ACCTBAL'] > avg_balances[cntrycode]:
        customer_orders = db.orders.find_one({'O_CUSTKEY': customer['C_CUSTKEY']})
        if not customer_orders:
            if cntrycode not in cust_sales:
                cust_sales[cntrycode] = {'NUMCUST': 0, 'TOTACCTBAL': 0}
            cust_sales[cntrycode]['NUMCUST'] += 1
            cust_sales[cntrycode]['TOTACCTBAL'] += customer['C_ACCTBAL']

# Sort results by country code
sorted_results = sorted(cust_sales.items())

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for country_code, data in sorted_results:
        writer.writerow({
            'CNTRYCODE': country_code,
            'NUMCUST': data['NUMCUST'],
            'TOTACCTBAL': data['TOTACCTBAL']
        })

# Close the client connection
client.close()
