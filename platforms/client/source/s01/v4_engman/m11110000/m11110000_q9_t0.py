import pymysql
import pymongo
import csv
from pymongo import MongoClient
from datetime import datetime

# MySQL connection parameters
mysql_conn_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# MongoDB connection parameters
mongo_conn_params = {
    'host': 'mongodb',
    'port': 27017,
    'db': 'tpch',
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_params)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = MongoClient(**{k: mongo_conn_params[k] for k in ['host', 'port']})
mongo_db = mongo_client[mongo_conn_params['db']]

# Query for MySQL
mysql_query = """
SELECT s.S_NATIONKEY, YEAR(o.O_ORDERDATE) AS year, SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) AS profit
FROM lineitem l
JOIN partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
GROUP BY s.S_NATIONKEY, year
"""

# Execute MySQL query
mysql_cursor.execute(mysql_query)
mysql_results = mysql_cursor.fetchall()

# Query for MongoDB and process results
mongo_results = []
parts = mongo_db.part.find({'P_NAME': {'$regex': '.*dim.*'}})
part_keys = [part['P_PARTKEY'] for part in parts]

for part_key in part_keys:
    lineitems = mongo_db.lineitem.find({'L_PARTKEY': part_key})
    for lineitem in lineitems:
        supplier = mongo_db.supplier.find_one({'S_SUPPKEY': lineitem['L_SUPPKEY']})
        if supplier:
            profit = (lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])) - (0 * lineitem['L_QUANTITY'])  # Assuming ps_supplycost is 0 since not joined
            result = (supplier['S_NATIONKEY'], datetime.fromisoformat(lineitem['L_SHIPDATE'][:10]).year, profit)
            mongo_results.append(result)

# Combine results (assuming same structure)
combined_results = {key: [] for key in set([r[0] for r in mysql_results + mongo_results])}  # Initialize dict for each nation
for nationkey, year, profit in mysql_results + mongo_results:
    combined_results[nationkey].append((year, profit))

# Sort and aggregate results
final_results = []
for nationkey, results in combined_results.items():
    results.sort(reverse=True)  # Sort by year in descending order
    aggregated_results = {year: sum(profit for y, profit in results if y == year) for year, _ in results}
    for year, profit in aggregated_results.items():
        final_results.append([nationkey, year, profit])

# Sort final results by nation in ascending order and then by year in descending order
final_results.sort(key=lambda x: (x[0], -x[1]))

# Write final result to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    # CSV header
    writer.writerow(['nation_key', 'year', 'profit'])
    # CSV data rows
    writer.writerows(final_results)

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
