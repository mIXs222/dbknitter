import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root', 
                             password='my-secret-pw', 
                             db='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Prepare the query for MySQL
mysql_query = """
SELECT n.N_NAME AS supplier_nation, 
       'unknown' AS customer_nation, 
       YEAR(o.O_ORDERDATE) AS year, 
       SUM(o.O_TOTALPRICE * (1 - l.L_DISCOUNT)) AS revenue
FROM nation n, orders o
WHERE n.N_NATIONKEY = 'unknown'
      AND o.O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY supplier_nation, year
ORDER BY supplier_nation, year;
"""

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()

# Fetch data from MongoDB with aggregation
pipeline = [
    {
        "$match": {
            "S_NATIONKEY": {"$in": ["INDIA", "JAPAN"]},
            "C_NATIONKEY": {"$in": ["INDIA", "JAPAN"]},
            "L_SHIPDATE": {"$gte": datetime(1995, 1, 1), "$lte": datetime(1996, 12, 31)}
        }
    },
    {
        "$project": {
            "supplier_nation": "$S_NATIONKEY",
            "customer_nation": "$C_NATIONKEY",
            "year": {"$year": "$L_SHIPDATE"},
            "revenue": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}
        }
    },
    {
        "$group": {
            "_id": {
                "supplier_nation": "$supplier_nation",
                "customer_nation": "$customer_nation",
                "year": "$year"
            },
            "revenue": {"$sum": "$revenue"}
        }
    },
    {
        "$sort": {"_id.supplier_nation": 1, "_id.customer_nation": 1, "_id.year": 1}
    }
]

mongo_results = list(mongodb.lineitem.aggregate(pipeline))

# Combine results from MySQL and MongoDB
combined_results = []
for result in mysql_results:
    combined_results.append(result)

for result in mongo_results:
    combined_results.append((
        result['_id']['supplier_nation'],
        result['_id']['customer_nation'],
        result['_id']['year'],
        result['revenue']
    ))

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['supplier_nation', 'customer_nation', 'year', 'revenue'])
    for row in combined_results:
        writer.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
