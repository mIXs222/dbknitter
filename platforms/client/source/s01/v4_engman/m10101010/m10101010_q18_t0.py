# query.py

import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Fetch orders from MongoDB where quantity > 300
mongo_orders = mongodb.orders.aggregate([
    {'$project': {
        '_id': 0,
        'O_TOTALPRICE': 1,
        'O_ORDERDATE': 1,
        'O_CUSTKEY': 1,
        'O_ORDERKEY': 1,
        'quantity': {'$sum': '$lineitem.L_QUANTITY'}
    }},
    {'$match': {'quantity': {'$gt': 300}}}
])

# MySQL query
mysql_cursor = mysql_conn.cursor()

mysql_query = """
SELECT C_NAME, C_CUSTKEY, 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'quantity'
FROM customer
WHERE C_CUSTKEY IN (%s)
"""

large_volume_customers = {order['O_CUSTKEY']: order for order in mongo_orders}
custkeys = ','.join(str(key) for key in large_volume_customers.keys())

mysql_cursor.execute(mysql_query % custkeys)
results = mysql_cursor.fetchall()

# We should filter the results again as only those with matching orders are to be considered
filtered_results = [row for row in results if row[1] in large_volume_customers]

# Sorting the results
sorted_results = sorted(filtered_results, key=lambda x: (-x[4], x[3]))

# Write to CSV
output_file = 'query_output.csv'
with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'quantity'])
    for row in sorted_results:
        order = large_volume_customers[row[1]]
        writer.writerow([row[0], row[1], order['O_ORDERKEY'], order['O_ORDERDATE'], order['O_TOTALPRICE'], order['quantity']])

# Close connections
mongo_client.close()
mysql_conn.close()
