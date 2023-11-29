# Python code (query.py)

import pymysql
import pymongo
import csv
from datetime import datetime

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']

# Execute query in mysql
sql_query = """
SELECT 
    c.C_CUSTKEY, 
    c.C_NAME, 
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue_lost, 
    c.C_ACCTBAL,
    c.C_ADDRESS,
    c.C_PHONE,
    c.C_COMMENT,
    n_nationkey
FROM customer c
JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN lineitem l ON l.L_ORDERKEY = o.O_ORDERKEY
WHERE l.L_RETURNFLAG = 'R'
AND o.O_ORDERDATE >= '1993-10-01'
AND o.O_ORDERDATE < '1994-01-01'
AND l.L_SHIPDATE < '1994-01-01'
GROUP BY c.C_CUSTKEY
ORDER BY revenue_lost ASC, c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL DESC;
"""

with mysql_conn.cursor() as mysql_cursor:
    mysql_cursor.execute(sql_query)
    mysql_result = mysql_cursor.fetchall()

# Retrieve nation data with pymongo
nation_data = {doc['N_NATIONKEY']: doc['N_NAME'] for doc in nation_collection.find({}, {'_id': 0})}

# Define output file
output_file = 'query_output.csv'

# Combine data and write to CSV
with open(output_file, 'w', newline='') as csvfile:
    fieldnames = ['C_CUSTKEY', 'C_NAME', 'revenue_lost', 'C_ACCTBAL', 'nation', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in mysql_result:
        writer.writerow({
            'C_CUSTKEY': row[0],
            'C_NAME': row[1],
            'revenue_lost': row[2],
            'C_ACCTBAL': row[3],
            'nation': nation_data.get(row[7], ''),
            'C_ADDRESS': row[4],
            'C_PHONE': row[5],
            'C_COMMENT': row[6]
        })

# Close connections
mysql_conn.close()
mongo_client.close()
