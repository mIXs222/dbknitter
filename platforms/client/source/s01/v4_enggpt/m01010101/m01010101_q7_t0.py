import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# MySQL query
mysql_query = """
SELECT
    n1.N_NAME as supplier_nation,
    n2.N_NAME as customer_nation,
    YEAR(o.O_ORDERDATE) as year,
    sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as revenue
FROM
    orders o
JOIN
    nation n2 ON o.O_CUSTKEY = n2.N_NATIONKEY
WHERE
    n2.N_NAME = 'INDIA' OR n2.N_NAME = 'JAPAN'
GROUP BY
    supplier_nation, customer_nation, year
ORDER BY
    supplier_nation, customer_nation, year;
"""

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()

# MongoDB query
lineitems = list(mongodb.lineitem.find(
    {
        '$and': [
            {'L_SHIPDATE': {'$gte': datetime(1995, 1, 1)}},
            {'L_SHIPDATE': {'$lte': datetime(1996, 12, 31)}}
        ]
    },
    {
        'L_ORDERKEY': 1,
        'L_EXTENDEDPRICE': 1,
        'L_DISCOUNT': 1,
        'L_SHIPDATE': 1
    }
))

# Additional MongoDB data processing
lineitem_data = {}
for lineitem in lineitems:
    year = lineitem['L_SHIPDATE'].year
    key = (lineitem['L_ORDERKEY'], year)
    revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    if key not in lineitem_data:
        lineitem_data[key] = revenue
    else:
        lineitem_data[key] += revenue

# Combine MySQL and MongoDB results
combined_result = []
for order in mysql_results:
    order_key_year = (order[3], order[2])
    if order_key_year in lineitem_data:
        combined_result.append(
            (order[0], order[1], order_key_year[1], lineitem_data[order_key_year])
        )

# Sort combined result
combined_result.sort(key=lambda x: (x[0], x[1], x[2]))

# Write result to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['supplier_nation', 'customer_nation', 'year', 'revenue'])
    for row in combined_result:
        csvwriter.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
