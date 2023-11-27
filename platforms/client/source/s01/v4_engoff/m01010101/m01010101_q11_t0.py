import pymysql
import pymongo
import pandas as pd
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_col = mongo_db['supplier']

# Fetch German supplier keys from MongoDB
german_suppliers = list(supplier_col.find({'S_NATIONKEY': 3}, {'S_SUPPKEY': 1, '_id': 0}))
german_suppliers_keys = [supplier['S_SUPPKEY'] for supplier in german_suppliers]

# Create the query for MySQL
mysql_query = '''
SELECT PS_PARTKEY, SUM(PS_AVAILQTY * PS_SUPPLYCOST) AS VALUE
FROM partsupp
WHERE PS_SUPPKEY IN (%s)
GROUP BY PS_PARTKEY
HAVING SUM(PS_AVAILQTY * PS_SUPPLYCOST) > 0.0001
ORDER BY VALUE DESC;
'''

# Format the list of supplier keys into a string for the query
formatted_supplier_keys = ', '.join(str(key) for key in german_suppliers_keys)

# Execute the query in MySQL
mysql_cursor = mysql_connection.cursor()
mysql_cursor.execute(mysql_query % formatted_supplier_keys)
query_results = mysql_cursor.fetchall()

# Close the MySQL connection
mysql_cursor.close()
mysql_connection.close()

# Write the query results to a CSV file
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['PS_PARTKEY', 'VALUE'])  # Writing headers
    for row in query_results:
        writer.writerow(row)
