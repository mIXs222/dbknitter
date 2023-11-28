import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("""
SELECT o.O_CUSTKEY, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE
FROM orders o
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE o.O_ORDERDATE BETWEEN '1993-10-01' AND '1993-12-31'
AND l.L_RETURNFLAG = 'R'
GROUP BY o.O_CUSTKEY
""")

# Fetch the results
mysql_results = mysql_cursor.fetchall()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_tpch = mongo_client['tpch']
customers_coll = mongo_tpch['customer']
nation_coll = mongo_tpch['nation']

customer_data_mapping = {}
for record in mysql_results:
    cust_key, revenue = record
    customer = customers_coll.find_one({"C_CUSTKEY": cust_key})
    nation = nation_coll.find_one({"N_NATIONKEY": customer["C_NATIONKEY"]})

    customer_data_mapping[cust_key] = {
        "C_CUSTKEY": cust_key,
        "C_NAME": customer["C_NAME"],
        "REVENUE": revenue,
        "C_ACCTBAL": customer['C_ACCTBAL'],
        "N_NAME": nation["N_NAME"],
        "C_ADDRESS": customer["C_ADDRESS"],
        "C_PHONE": customer["C_PHONE"],
        "C_COMMENT": customer["C_COMMENT"],
    }

# Sort the results as required
sorted_results = sorted(customer_data_mapping.values(), key=lambda x: (x['REVENUE'], x['C_CUSTKEY'], x['C_NAME'], -x['C_ACCTBAL']))

# Write to csv file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for row in sorted_results:
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
