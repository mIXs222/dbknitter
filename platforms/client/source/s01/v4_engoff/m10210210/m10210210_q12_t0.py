# Import required packages
import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']

# Prepare the queries
mysql_query = """
SELECT L_SHIPMODE, COUNT(*) as late_lineitems_count, L_ORDERKEY
FROM lineitem
WHERE L_SHIPDATE < L_COMMITDATE
AND L_RECEIPTDATE > L_COMMITDATE
AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
AND (L_SHIPMODE = 'MAIL' OR L_SHIPMODE = 'SHIP')
GROUP BY L_SHIPMODE, L_ORDERKEY
"""

# Execute the MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()

# Fetch the order priorities from MongoDB
mongo_results = list(orders_collection.find(
    {
        "O_ORDERKEY": {"$in": [row[2] for row in mysql_results]},
    },
    {
        "O_ORDERKEY": 1, "O_ORDERPRIORITY": 1
    }
))

# Classify the shipping priorities
urgency_map = {}
for order in mongo_results:
    urgency_map[order['O_ORDERKEY']] = "URGENT_OR_HIGH" if order['O_ORDERPRIORITY'] in ['URGENT', 'HIGH'] else "OTHER"

# Combine the results from both databases
final_results = []
for shipmode, late_lineitems_count, orderkey in mysql_results:
    order_priority_group = urgency_map.get(orderkey, "UNKNOWN")
    final_results.append((shipmode, order_priority_group, late_lineitems_count))

# Sort the results
final_results.sort()

# Write the results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    for line in final_results:
        writer.writerow(line)

# Close connections
mysql_conn.close()
mongo_client.close()
