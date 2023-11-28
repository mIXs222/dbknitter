import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']

# Query for nation from MySQL
mysql_cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY';")
nation_key = mysql_cursor.fetchone()[0]

# Query for suppliers from MongoDB
german_suppliers = list(supplier_collection.find({'S_NATIONKEY': nation_key}))

# Filter supplier keys for query condition
german_supplier_keys = [supplier['S_SUPPKEY'] for supplier in german_suppliers]

# Start building up the SQL query
base_query = """
SELECT PS_PARTKEY, SUM(PS_SUPPLYCOST * PS_AVAILQTY) as total_value
FROM partsupp
WHERE PS_SUPPKEY IN %s
GROUP BY PS_PARTKEY
HAVING total_value > (
    SELECT 0.05 * SUM(PS_SUPPLYCOST * PS_AVAILQTY)
    FROM partsupp 
    WHERE PS_SUPPKEY IN %s)  -- Placeholder for a real threshold value
ORDER BY total_value DESC;
"""

# Execute the query in MySQL
mysql_cursor.execute(base_query, (german_supplier_keys, german_supplier_keys))

# Fetching the results
results = mysql_cursor.fetchall()

# Writing results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['PS_PARTKEY', 'TOTAL_VALUE'])
    csvwriter.writerows(results)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
