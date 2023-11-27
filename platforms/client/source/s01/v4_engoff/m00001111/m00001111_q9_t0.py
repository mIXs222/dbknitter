import pymysql
import pymongo
import csv
from datetime import datetime

# Function to execute the query on MySQL and return the results
def mysql_query(sql, connection_params):
    try:
        connection = pymysql.connect(**connection_params)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()
    finally:
        connection.close()

# Function to execute the query on MongoDB and return the results
def mongo_query(query, db, collection_name):
    collection = db[collection_name]
    return list(collection.aggregate(query))

# MySQL connection parameters
mysql_conn_params = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch",
}

# MongoDB connection parameters
mongo_conn_params = {
    "port": 27017,
    "host": "mongodb",
}

# Connect to MongoDB
mongo_client = pymongo.MongoClient(**mongo_conn_params)
mongo_db = mongo_client['tpch']

# Query for MySQL
mysql_sql = """
SELECT n.N_NAME AS nation, YEAR(o.O_ORDERDATE) AS o_year, SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) AS profit
FROM nation n
JOIN supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
JOIN lineitem l ON ps.PS_PARTKEY = l.L_PARTKEY AND ps.PS_SUPPKEY = l.L_SUPPKEY
JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
JOIN part p ON l.L_PARTKEY = p.P_PARTKEY
WHERE p.P_NAME LIKE '%dim%' AND o.O_ORDERDATE >= '1992-01-01' AND o.O_ORDERDATE < '1997-12-31'
GROUP BY nation, o_year
ORDER BY nation, o_year DESC;
"""

mysql_results = mysql_query(mysql_sql, mysql_conn_params)

# Query for MongoDB
mongo_query = [
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'PS_PARTKEY',
            'foreignField': 'L_PARTKEY',
            'as': 'lineitems'
        }
    },
    {
        '$unwind': '$lineitems'
    },
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'lineitems.L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'orders'
        }
    },
    {
        '$unwind': '$orders'
    },
    {
        '$match': {
            'orders.O_ORDERDATE': {
                '$gte': datetime(1995, 1, 1),
                '$lt': datetime(1996, 1, 1)
            }
        }
    },
    {
        '$group': {
            '_id': {
                'PS_SUPPKEY': '$PS_SUPPKEY',
                'year': {'$year': '$orders.O_ORDERDATE'}
            },
            'profit': {
                '$sum': {
                    '$subtract': [
                        {'$multiply': ['$lineitems.L_EXTENDEDPRICE', {'$subtract': [1, '$lineitems.L_DISCOUNT']}]},
                        {'$multiply': ['$PS_SUPPLYCOST', '$lineitems.L_QUANTITY']}
                    ]
                }
            }
        }
    },
]

mongo_results = mongo_query(mongo_query, mongo_db, 'partsupp')

# Combine the results from MySQL and MongoDB
combined_results = mysql_results  # Start with MySQL results

# Add MongoDB results
for res in mongo_results:
    nation = res['_id']['PS_SUPPKEY']  # This should be updated to retrieve the actual nation based on the PS_SUPPKEY mapping
    o_year = res['_id']['year']
    profit = res['profit']
    combined_results.append((nation, o_year, profit))

# Sort combined results as per the requirement (nation ascending, year descending)
combined_results.sort(key=lambda x:(x[0], -x[1]))

# Write the results to the file query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Nation', 'Year', 'Profit'])
    for result in combined_results:
        writer.writerow(result)
