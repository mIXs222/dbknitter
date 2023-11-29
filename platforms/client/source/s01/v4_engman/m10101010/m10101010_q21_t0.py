# suppliers_who_kept_orders_waiting.py

import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# MongoDB query to match nation 'SAUDI ARABIA' and get corresponding nation key
nation_key = mongodb_db['nation'].find_one({'N_NAME': 'SAUDI ARABIA'}, {'N_NATIONKEY': 1})['N_NATIONKEY']

# MySQL query to fetch suppliers from nation 'SAUDI ARABIA' and their lineitem details
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("""
SELECT s.S_NAME, COUNT(*) as NUMWAIT
FROM supplier s
JOIN lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
WHERE s.S_NATIONKEY = %s AND l.L_RETURNFLAG = 'F' AND l.L_COMMITDATE < l.L_RECEIPTDATE
GROUP BY s.S_SUPPKEY
HAVING NUMWAIT > 1
ORDER BY NUMWAIT DESC, s.S_NAME
""", (nation_key,))

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['NUMWAIT', 'S_NAME'])
    for row in mysql_cursor:
        csvwriter.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
