import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# SQL Query for MySQL Data
mysql_query = '''
SELECT n.N_NAME AS nation, 
       YEAR(o.O_ORDERDATE) AS o_year, 
       SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) - ps.PS_SUPPLYCOST * l.L_QUANTITY) AS profit
FROM lineitem l
JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
JOIN partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY
JOIN part p ON p.P_PARTKEY = l.L_PARTKEY
JOIN nation n ON n.N_NATIONKEY = ps.PS_SUPPKEY
WHERE p.P_NAME LIKE '%dim%'
GROUP BY nation, o_year 
ORDER BY nation ASC, o_year DESC;
'''

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()

# MongoDB Query for documents in 'supplier' Collection
pipeline = [
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'S_SUPPKEY',
            'foreignField': 'L_SUPPKEY',
            'as': 'lineitems'
        }
    },
    {'$unwind': '$lineitems'},
    {'$match': {'lineitems.L_PARTKEY': {'$regex': '.*dim.*'}}},
    {
        '$project': {
            'nation': '$S_NAME',
            'o_year': {'$year': '$lineitems.L_SHIPDATE'},
            'profit': {
                '$subtract': [
                    {'$multiply': ['$lineitems.L_EXTENDEDPRICE', {'$subtract': [1, '$lineitems.L_DISCOUNT']}]},
                    {'$multiply': ['$lineitems.L_QUANTITY', '$lineitems.PS_SUPPLYCOST']}
                ]
            }
        }
    },
    {
        '$group': {
            '_id': {'nation': '$nation', 'o_year': '$o_year'},
            'profit': {'$sum': '$profit'}
        }
    },
    {
        '$sort': {'_id.nation': 1, '_id.o_year': -1}
    }
]

# Execute MongoDB aggregation
mongo_results = list(mongodb['supplier'].aggregate(pipeline))

# Combine results from MySQL and MongoDB
combined_results = mysql_results + [
    (doc['_id']['nation'], doc['_id']['o_year'], doc['profit']) for doc in mongo_results
]

# Sort combined results by nation and year as required
combined_results.sort(key=lambda x: (x[0], -x[1]))

# Write results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Write the headers
    csvwriter.writerow(['nation', 'year', 'profit'])
    # Write the data
    for row in combined_results:
        csvwriter.writerow(row)

# Close the connections
mysql_conn.close()
mongo_client.close()
