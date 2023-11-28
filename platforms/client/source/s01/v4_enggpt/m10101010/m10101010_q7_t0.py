import csv
import pymysql
from pymongo import MongoClient
from datetime import datetime

# Initialize MySQL connection
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch",
    charset='utf8mb4'
)

# Initialize MongoDB connection
mongo_client = MongoClient("mongodb", 27017)
mongodb = mongo_client["tpch"]

# MySQL query to get supplier, customer, and lineitem data for the given conditions
mysql_query = """
SELECT 
    s.S_NATIONKEY AS supplier_nationkey,
    c.C_NATIONKEY AS customer_nationkey,
    YEAR(l.L_SHIPDATE) AS year_of_shipping,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
FROM 
    supplier s 
JOIN 
    lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
JOIN 
    customer c ON l.L_ORDERKEY = c.C_CUSTKEY
WHERE 
    s.S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'JAPAN' OR N_NAME = 'INDIA') 
    AND c.C_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'JAPAN' OR N_NAME = 'INDIA') 
    AND l.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY 
    supplier_nationkey, customer_nationkey, year_of_shipping
ORDER BY 
    supplier_nationkey, customer_nationkey, year_of_shipping;
"""

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()

# Process supplier and customer nation names
def get_nation_name(nationkey):
    nation_doc = mongodb.nation.find_one({"N_NATIONKEY": nationkey})
    return nation_doc["N_NAME"] if nation_doc else None

# Final result list to include headers
final_results = [("supplier_nation", "customer_nation", "year_of_shipping", "revenue")]

# Process results from MySQL
for row in mysql_results:
    supplier_nation = get_nation_name(row[0])
    customer_nation = get_nation_name(row[1])
    # Append result tuple to final results
    final_results.append((supplier_nation, customer_nation, row[2], row[3]))

# Write the results to a CSV file
with open("query_output.csv", "w", newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(final_results)

# Clean up connections
mysql_conn.close()
mongo_client.close()
