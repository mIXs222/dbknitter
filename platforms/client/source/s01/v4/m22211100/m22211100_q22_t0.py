# query.py

import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customer_collection = mongo_db['customer']

# Perform the subquery to get average account balance from MongoDB
avg_account_balance_pipeline = [
    {"$match": {"C_ACCTBAL": {"$gt": 0.00}, "C_PHONE": {"$regex": "^(20|40|22|30|39|42|21)"}}},
    {"$group": {"_id": None, "avg_acctbal": {"$avg": "$C_ACCTBAL"}}}
]
avg_result = list(customer_collection.aggregate(avg_account_balance_pipeline))
avg_account_balance = avg_result[0]['avg_acctbal'] if avg_result else 0

# Get all customer keys from SQL 'orders' for exclusion
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT DISTINCT O_CUSTKEY FROM orders")
    excluded_custkeys = [row[0] for row in cursor.fetchall()]

# Final result storage
results = {}

# Perform the final query: Fetch from MongoDB only customers with balance greater than average calculated above
matching_customers_pipeline = [
    {"$match": {
        "C_ACCTBAL": {"$gt": avg_account_balance},
        "C_PHONE": {"$regex": "^(20|40|22|30|39|42|21)"},
        "C_CUSTKEY": {"$nin": excluded_custkeys}
    }},
    {"$project": {"CNTRYCODE": {"$substr": ["$C_PHONE", 0, 2]}, "C_ACCTBAL": 1, "_id": 0}}
]
matching_customers = customer_collection.aggregate(matching_customers_pipeline)

# Process the MongoDB customers and aggregate by country code
for cust in matching_customers:
    cntrycode = cust['CNTRYCODE']
    acctbal = cust['C_ACCTBAL']
    if cntrycode not in results:
        results[cntrycode] = {'NUMCUST': 1, 'TOTACCTBAL': acctbal}
    else:
        results[cntrycode]['NUMCUST'] += 1
        results[cntrycode]['TOTACCTBAL'] += acctbal

# Write to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL'])
    
    for cntrycode in sorted(results.keys()):
        writer.writerow([cntrycode, results[cntrycode]['NUMCUST'], results[cntrycode]['TOTACCTBAL']])

# Close the connections
mysql_conn.close()
