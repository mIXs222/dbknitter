import pymongo
import csv
from decimal import Decimal

# Connect to MongoDB
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client['tpch']
customer_collection = db['customer']
orders_collection = db['orders']

# Determine the average account balance for selected country codes with positive balance
avg_balances = {}
for cntrycode in ['20', '40', '22', '30', '39', '42', '21']:
    avg_balance = customer_collection.aggregate([
        {"$match": {"C_ACCTBAL": {"$gt": 0}, "C_PHONE": {"$regex": f"^{cntrycode}"}}},
        {"$group": {"_id": None, "average_balance": {"$avg": "$C_ACCTBAL"}}}
    ])
    avg_bal = list(avg_balance)
    if avg_bal:
        avg_balances[cntrycode] = avg_bal[0]['average_balance']

# Retrieve customers based on criteria
custsales = []
for cntrycode, avg_bal in avg_balances.items():
    customers = customer_collection.find({
        "C_PHONE": {"$regex": f"^{cntrycode}"},
        "C_ACCTBAL": {"$gt": avg_bal}
    })

    for customer in customers:
        # Check if customer has no orders
        if not orders_collection.find_one({"O_CUSTKEY": customer["C_CUSTKEY"]}):
            cntrycode = customer['C_PHONE'][:2]
            custsale = {
                'CNTRYCODE': cntrycode,
                'NUMCUST': 1,
                'C_CUSTKEY': customer['C_CUSTKEY'],
                'TOTACCTBAL': customer['C_ACCTBAL']
            }
            custsales.append(custsale)

# Group by country codes and aggregate the results
result = {}
for custsale in custsales:
    cntrycode = custsale['CNTRYCODE']
    if cntrycode not in result:
        result[cntrycode] = {'NUMCUST': 0, 'TOTACCTBAL': Decimal('0.0')}
    result[cntrycode]['NUMCUST'] += 1
    result[cntrycode]['TOTACCTBAL'] += Decimal(custsale['TOTACCTBAL'])

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for cntrycode in sorted(result):
        writer.writerow({
            'CNTRYCODE': cntrycode,
            'NUMCUST': result[cntrycode]['NUMCUST'],
            'TOTACCTBAL': float(result[cntrycode]['TOTACCTBAL'])
        })
