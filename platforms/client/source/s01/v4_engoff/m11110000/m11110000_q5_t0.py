import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get ASIA nations from MongoDB
asia_nations = list(mongo_db.nation.find({
    'N_REGIONKEY': {'$in': [r['R_REGIONKEY'] for r in mongo_db.region.find({'R_NAME': 'ASIA'})]}
}, {'_id': 0, 'N_NATIONKEY': 1}))

asia_nation_keys = [n['N_NATIONKEY'] for n in asia_nations]

# Prepare query for MySQL
query = """
SELECT n.N_NAME as nation_name, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as revenue_volume
FROM customer c
JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY AND o.O_ORDERDATE >= '1990-01-01' AND o.O_ORDERDATE < '1995-01-01'
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY AND s.S_NATIONKEY IN (%s)
JOIN nation n ON c.C_NATIONKEY = n.N_NATIONKEY
WHERE c.C_NATIONKEY IN (%s)
GROUP BY nation_name
ORDER BY revenue_volume DESC
"""

# Since we cannot bind a list of integers as a parameter, we're preparing the IN clause manually
in_clause = ', '.join(['%s'] * len(asia_nation_keys))
formatted_query = query % (in_clause, in_clause)
params = asia_nation_keys * 2

# Execute query on MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute(formatted_query, params)
    results = cursor.fetchall()

# Write the query results to the file
with open('query_output.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    
    # Write the headers
    csv_writer.writerow(['nation_name', 'revenue_volume'])
    
    for row in results:
        csv_writer.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
