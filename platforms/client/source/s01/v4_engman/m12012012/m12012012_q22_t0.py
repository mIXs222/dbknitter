import pymysql
import pymongo
from datetime import datetime, timedelta
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client['tpch']

# Customer data retrieval from MySQL
customer_data_query = """
SELECT C_CUSTKEY, C_PHONE, C_ACCTBAL
FROM customer
WHERE SUBSTRING(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
  AND C_ACCTBAL > 0
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(customer_data_query)
    customer_data = [row for row in cursor]

# Find average account balance by country code
average_acct_balances = {}
for code in ['20', '40', '22', '30', '39', '42', '21']:
    acc_balances = [cust[2] for cust in customer_data if cust[1].startswith(code)]
    if acc_balances:
        average_acct_balances[code] = sum(acc_balances) / len(acc_balances)

# Get orders from MongoDB
seven_years_ago = datetime.now() - timedelta(days=7*365)
orders_data = mongodb_db.orders.find({
    "O_ORDERDATE": {"$lte": seven_years_ago}
}, {"O_CUSTKEY": 1})

# Customer keys with orders older than 7 years
cust_keys_order_older_than_7_years = set(order['O_CUSTKEY'] for order in orders_data)

# Count customers and sum balances
results = []
for code in ['20', '40', '22', '30', '39', '42', '21']:
    number_of_customers = 0
    total_balance = 0.0
    for cust in customer_data:
        if cust[1].startswith(code) and cust[0] not in cust_keys_order_older_than_7_years:
            if cust[2] > average_acct_balances[code]:
                number_of_customers += 1
                total_balance += cust[2]
    if number_of_customers > 0:
        results.append((code, number_of_customers, total_balance))

# Save results to CSV
results.sort(key=lambda x: x[0])
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['CNTRYCODE', 'CUSTOMER_COUNT', 'TOTAL_ACCTBAL'])
    for result in results:
        writer.writerow(result)

# Close connections
mysql_conn.close()
mongodb_client.close()
