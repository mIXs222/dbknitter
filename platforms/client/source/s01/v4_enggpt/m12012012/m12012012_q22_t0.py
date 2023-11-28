import csv
import pymysql
import pymongo
from pymongo import MongoClient

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch customers with account balance greater than the average positive balance for specified country codes
mysql_cursor.execute("""
SELECT
    SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE,
    C_CUSTKEY,
    C_ACCTBAL
FROM
    customer
HAVING
    CNTRYCODE IN ('20', '40', '22', '30', '39', '42', '21')
    AND C_ACCTBAL > (
        SELECT AVG(C_ACCTBAL) FROM customer WHERE C_ACCTBAL > 0 AND SUBSTR(C_PHONE, 1, 2) = CNTRYCODE
    ) AND NOT EXISTS (
        SELECT O_ORDERKEY FROM orders WHERE customer.C_CUSTKEY = orders.O_CUSTKEY
    )
ORDER BY
    CNTRYCODE ASC
""")
customers = mysql_cursor.fetchall()

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# Filter out customers who have orders
customers_without_orders = []
for cust in customers:
    if not orders_collection.find_one({'O_CUSTKEY': cust[1]}):
        customers_without_orders.append(cust)

# Aggregate the results
result = {}
for cust in customers_without_orders:
    country_code = cust[0]
    acct_balance = cust[2]
    if country_code not in result:
        result[country_code] = {'NUMCUST': 0, 'TOTACCTBAL': 0}
    result[country_code]['NUMCUST'] += 1
    result[country_code]['TOTACCTBAL'] += acct_balance

# Write results to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL'])
    for cntrycode in sorted(result):
        writer.writerow([cntrycode, result[cntrycode]['NUMCUST'], round(result[cntrycode]['TOTACCTBAL'], 2)])

# Close MongoDB connection
mongo_client.close()
