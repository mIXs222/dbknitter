import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongodb_client["tpch"]
nation_collection = mongodb["nation"]
orders_collection = mongodb["orders"]

# Query from MySQL to fetch supplier and customer data for nations India and Japan
supplier_nations = ['INDIA', 'JAPAN']
nation_indexes = {}

# Fetch nations related to supplier and customer from MongoDB
for nation in nation_collection.find({'N_NAME': {'$in': supplier_nations}}):
    nation_indexes[nation['N_NAME']] = nation['N_NATIONKEY']

# Nation keys for India and Japan
india_nationkey = nation_indexes['INDIA']
japan_nationkey = nation_indexes['JAPAN']

# Fetch lineitem information within the year range 1995-1996 from MySQL for suppliers from India and Japan
lineitem_query = """
SELECT
    s.S_NATIONKEY as supplier_nationkey,
    c.C_NATIONKEY as customer_nationkey,
    YEAR(l.L_SHIPDATE) as year,
    (l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as revenue
FROM
    lineitem l
JOIN
    supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
JOIN
    customer c ON l.L_ORDERKEY IN (SELECT o.O_ORDERKEY FROM orders o WHERE o.O_CUSTKEY = c.C_CUSTKEY)
WHERE
    s.S_NATIONKEY IN (%s, %s)
    AND c.C_NATIONKEY IN (%s, %s)
    AND l.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
"""
mysql_cursor.execute(lineitem_query, (india_nationkey, japan_nationkey, india_nationkey, japan_nationkey))

# Collect the results
results = []

for row in mysql_cursor:
    supplier_nation = 'INDIA' if row[0] == india_nationkey else 'JAPAN'
    customer_nation = 'INDIA' if row[1] == india_nationkey else 'JAPAN'
    year = row[2]
    revenue = row[3]
    results.append((supplier_nation, customer_nation, year, revenue))

# Sort results
results = sorted(results, key=lambda x: (x[0], x[1], x[2]))

# Write the outputs to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR', 'REVENUE'])
    for data in results:
        writer.writerow(data)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
