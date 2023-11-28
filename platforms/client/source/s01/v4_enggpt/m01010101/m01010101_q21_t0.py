import pymysql
import pymongo
import csv

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
cursor = mysql_conn.cursor()

# Connection to MongoDB
mongoclient = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongoclient['tpch']

# Query for MySQL database to get suppliers in Saudi Arabia
cursor.execute("""
SELECT s.S_NAME, n.N_NAME
FROM supplier AS s
JOIN nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE n.N_NAME = 'SAUDI ARABIA';
""")
suppliers_in_saudi_arabia = cursor.fetchall()

supplier_waiting_time = []
# Loop over each supplier to get the waiting time
for supplier_name, _ in suppliers_in_saudi_arabia:
    cursor.execute("""
    SELECT COUNT(*)
    FROM orders AS o
    JOIN lineitem AS l1 ON o.O_ORDERKEY = l1.L_ORDERKEY
    WHERE l1.L_SUPPKEY = (SELECT S_SUPPKEY FROM supplier WHERE S_NAME = %s) 
    AND o.O_ORDERSTATUS = 'F'
    AND l1.L_RECEIPTDATE > l1.L_COMMITDATE
    AND EXISTS (
        SELECT 1
        FROM lineitem AS l2
        WHERE l2.L_ORDERKEY = l1.L_ORDERKEY
        AND l2.L_SUPPKEY != l1.L_SUPPKEY
    )
    AND NOT EXISTS (
        SELECT 1
        FROM lineitem AS l3
        WHERE l3.L_ORDERKEY = l1.L_ORDERKEY
        AND l3.L_SUPPKEY != l1.L_SUPPKEY
        AND l3.L_RECEIPTDATE > l1.L_COMMITDATE
    );""", (supplier_name,))
    num_wait = cursor.fetchone()[0]
    supplier_waiting_time.append((supplier_name, num_wait))

# Close MySQL connection
cursor.close()
mysql_conn.close()

# Sort supplier waiting times based on the count and then by supplier name
supplier_waiting_time.sort(key=lambda x: (-x[1], x[0]))

# Write the result into a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_NAME', 'NUMWAIT'])
    for row in supplier_waiting_time:
        writer.writerow(row)
