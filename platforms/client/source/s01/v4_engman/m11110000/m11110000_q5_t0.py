# query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Get nation keys for nations in the ASIA region from MongoDB
asia_nations = mongodb['nation'].aggregate([
    {
        '$lookup': {
            'from': 'region',
            'localField': 'N_REGIONKEY',
            'foreignField': 'R_REGIONKEY',
            'as': 'region'
        }
    },
    {'$match': {'region.R_NAME': 'ASIA'}},
    {'$project': {'N_NATIONKEY': 1, '_id': 0}}
])

asia_nation_keys = [nation['N_NATIONKEY'] for nation in asia_nations]

# Prepare the SQL query
sql = '''
SELECT n.N_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE
FROM customer c
JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE c.C_NATIONKEY = s.S_NATIONKEY AND
      n.N_NATIONKEY IN (%s) AND
      o.O_ORDERDATE >= '1990-01-01' AND
      o.O_ORDERDATE < '1995-01-01'
GROUP BY n.N_NAME
ORDER BY REVENUE DESC;
''' % ','.join(map(str, asia_nation_keys))

# Execute the SQL query
mysql_cursor.execute(sql)
results = mysql_cursor.fetchall()

# Write the query's output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['N_NAME', 'REVENUE'])
    for row in results:
        csv_writer.writerow(row)

# Clean up
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
