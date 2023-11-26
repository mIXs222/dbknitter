import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn_info = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch",
}

mysql_conn = pymysql.connect(**mysql_conn_info)
mysql_cursor = mysql_conn.cursor()

# Run the query for MySQL
mysql_query = """
SELECT
    PS_PARTKEY,
    S_SUPPKEY,
    PS_SUPPLYCOST * PS_AVAILQTY AS PART_VALUE
FROM partsupp, supplier
WHERE PS_SUPPKEY = S_SUPPKEY;
"""
mysql_cursor.execute(mysql_query)
mysql_results = mysql_cursor.fetchall()

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB
mongodb_conn_info = {
    "host": "mongodb",
    "port": 27017,
}

mongodb_conn = pymongo.MongoClient(**mongodb_conn_info)
mongodb_db = mongodb_conn["tpch"]
nation_collection = mongodb_db["nation"]

# Get the N_NATIONKEY for 'GERMANY'
nation_document = nation_collection.find_one({"N_NAME": "GERMANY"})
germany_nation_key = nation_document["N_NATIONKEY"]

# Filter MySQL results based on N_NATIONKEY
filtered_mysql_results = [row for row in mysql_results if row[1] == germany_nation_key]

# Compute SUM(PS_SUPPLYCOST * PS_AVAILQTY) and apply HAVING condition
sum_value = sum([row[2] for row in filtered_mysql_results]) * 0.0001000000
filtered_results_with_value = [(row[0], row[2]) for row in filtered_mysql_results if row[2] > sum_value]

# Sort the rows by VALUE in descending order
sorted_results = sorted(filtered_results_with_value, key=lambda x: x[1], reverse=True)

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['PS_PARTKEY', 'VALUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in sorted_results:
        writer.writerow({'PS_PARTKEY': row[0], 'VALUE': row[1]})
