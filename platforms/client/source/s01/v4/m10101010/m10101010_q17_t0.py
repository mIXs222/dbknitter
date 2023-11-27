# mysql_mongo_query.py
import pymongo
import pymysql
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_part_collection = mongo_db["part"]

# Perform subquery on MySQL (to fetch average quantities per part)
subquery_sql = """
    SELECT L_PARTKEY, 0.2 * AVG(L_QUANTITY) as AVG_QUANTITY
    FROM lineitem
    GROUP BY L_PARTKEY
"""
mysql_cursor.execute(subquery_sql)
average_quantities = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Perform MongoDB query to match the conditions and get part keys
part_keys = mongo_part_collection.find(
    {"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"}, {"P_PARTKEY": 1}
)

# Convert pymongo cursor to list of matching part keys
part_keys_list = [p["P_PARTKEY"] for p in part_keys]

# Main MySQL query to calculate SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
main_query_sql = """
    SELECT SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
    FROM lineitem
    WHERE L_PARTKEY IN %s AND L_QUANTITY < %s
"""

# Results list to store the intermediate results
results = []

# Execute main query for each matching part
for part_key in part_keys_list:
    if part_key in average_quantities:
        mysql_cursor.execute(main_query_sql, (part_key, average_quantities[part_key]))
        result = mysql_cursor.fetchone()
        if result:
            results.append(result[0])

# Calculate final result
final_result = sum(results) if results else 0

# Write result to a CSV file
with open('query_output.csv', 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['AVG_YEARLY'])
    writer.writerow([final_result])

# Close all the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
