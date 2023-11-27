import csv
from pymongo import MongoClient
import numpy as np

mongo_client = MongoClient('localhost', 27017)
db = mongo_client['tpch']

customers = db['customer']
orders = db['orders']

country_codes = ['20', '40', '22', '30', '39', '42', '21']

matched_custs = customers.find({
    "C_PHONE": { "$regex": "^" + "|^".join(country_codes) },
    "C_ACCTBAL": { "$gt": 0 }
})

avg_acctbal = np.mean([cust["C_ACCTBAL"] for cust in matched_custs])

matched_custs = customers.find({
    "C_PHONE": { "$regex": "^" + "|^".join(country_codes) },
    "C_ACCTBAL": { "$gt": avg_acctbal },
    "C_CUSTKEY": { "$nin": [order["O_CUSTKEY"] for order in orders.find()] }
})

result = {}

for cust in matched_custs:
    cntrycode = cust["C_PHONE"][:2]
    if cntrycode not in result:
        result[cntrycode] = {"CNTRYCODE": cntrycode, "NUMCUST": 0, "TOTACCTBAL": 0}
    result[cntrycode]['NUMCUST'] += 1
    result[cntrycode]['TOTACCTBAL'] += cust["C_ACCTBAL"]

with open('query_output.csv', 'w', newline='') as outfile:
    writer = csv.DictWriter(outfile, result[country_codes[0]].keys())
    writer.writeheader()
    for row in result.values():
        writer.writerow(row)
