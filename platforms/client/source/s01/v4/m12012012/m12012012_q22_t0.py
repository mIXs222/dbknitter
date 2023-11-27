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
mysql_cursor = mysql_conn.cursor()

# Fetch customers from MySQL who fulfill the criteria
mysql_query = """
SELECT
    C_CUSTKEY, SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE, C_ACCTBAL
FROM
    customer
WHERE
    SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
AND C_ACCTBAL > (
        SELECT AVG(C_ACCTBAL) FROM customer WHERE C_ACCTBAL > 0.00
        AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
)
"""
mysql_cursor.execute(mysql_query)
eligible_customers = mysql_cursor.fetchall()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# Filter out customers with orders
ids_with_orders = set(
    doc['O_CUSTKEY'] for doc in orders_collection.find({}, {'O_CUSTKEY': 1})
)
result_customers = [cust for cust in eligible_customers if cust[0] not in ids_with_orders]

# Aggregate results
aggregated_result = {}
for cust in result_customers:
    cntrycode = cust[1]
    acctbal = cust[2]
    if cntrycode not in aggregated_result:
        aggregated_result[cntrycode] = {'NUMCUST': 0, 'TOTACCTBAL': 0}
    aggregated_result[cntrycode]['NUMCUST'] += 1
    aggregated_result[cntrycode]['TOTACCTBAL'] += acctbal

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for cntrycode, values in aggregated_result.items():
        writer.writerow({
            'CNTRYCODE': cntrycode,
            'NUMCUST': values['NUMCUST'],
            'TOTACCTBAL': values['TOTACCTBAL']
        })

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
