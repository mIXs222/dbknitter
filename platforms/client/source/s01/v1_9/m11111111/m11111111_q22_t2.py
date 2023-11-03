import pymongo
import pandas as pd

client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]
customer_col = db["customer"]
orders_col = db["orders"]

cntry_codes = ['20', '40', '22', '30', '39', '42', '21']

# get the average C_ACCTBAL
cust_data = list(
    customer_col.find({"C_PHONE": {"$in": cntry_codes}, "C_ACCTBAL": {"$gt": 0.00}}))
avg_acctbal = sum(cust['C_ACCTBAL'] for cust in cust_data) / len(cust_data)

# get all customers without orders
no_orders_custkeys = {order['O_CUSTKEY']: None for order in orders_col.find()}
cust_data = [cust for cust in cust_data
             if cust['C_CUSTKEY'] not in no_orders_custkeys and cust['C_ACCTBAL'] > avg_acctbal]

result_data = []
for cntry_code in cntry_codes:
    customers = [cust for cust in cust_data if cust['C_PHONE'].startswith(cntry_code)]
    if customers:
        result_data.append({
            'CNTRYCODE': cntry_code,
            'NUMCUST': len(customers),
            'TOTACCTBAL': sum(cust['C_ACCTBAL'] for cust in customers)
        })

# sort by 'CNTRYCODE'
result_data.sort(key=lambda x: x['CNTRYCODE'])

# write output to csv file
df = pd.DataFrame(result_data)
df.to_csv('query_output.csv', index=False)
