import pymysql
import pymongo
import csv
import os

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4')

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# MySQL cursor
cursor = mysql_conn.cursor()

# Retrieve German nation key
nation_key = None
for nation in mongodb_db.nation.find({'N_NAME': 'GERMANY'}):
    nation_key = nation['N_NATIONKEY']
    break

if nation_key is None:
    print("No nation found for Germany.")
    exit()

# Perform MySQL query
query = """
SELECT ps.PS_PARTKEY, SUM(ps.PS_SUPPLYCOST * ps.PS_AVAILQTY) AS TotalValue
FROM partsupp ps
INNER JOIN supplier s ON ps.PS_SUPPKEY = s.S_SUPPKEY
WHERE s.S_NATIONKEY = %s
GROUP BY ps.PS_PARTKEY
HAVING TotalValue > (
    SELECT SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.05 -- Example percentage threshold
    FROM partsupp
    INNER JOIN supplier ON partsupp.PS_SUPPKEY = supplier.S_SUPPKEY
    WHERE supplier.S_NATIONKEY = %s
)
ORDER BY TotalValue DESC
"""

cursor.execute(query, (nation_key, nation_key))

# Output results to file
output_file = 'query_output.csv'
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['PARTKEY', 'TOTAL_VALUE'])
    for row in cursor.fetchall():
        writer.writerow(row)

# Close connections
cursor.close()
mysql_conn.close()
mongodb_client.close()
