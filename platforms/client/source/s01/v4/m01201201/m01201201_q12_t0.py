# query.py

import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to the MySQL server
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to the MongoDB server
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Select orders from the MySQL database
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(
    """
    SELECT O_ORDERKEY, O_ORDERPRIORITY
    FROM orders
    """
)

# Fetch orders from MySQL DBMS
order_priorities = {}
for (o_orderkey, o_orderpriority) in mysql_cursor:
    order_priorities[o_orderkey] = o_orderpriority

# Close the MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Prepare the query for MongoDB
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)
query = {
    'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
    'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'},
    'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
    'L_RECEIPTDATE': {'$gte': start_date, '$lt': end_date}
}

# Fetch lineitems from MongoDB
lineitems = mongodb_db.lineitem.find(query)


# Prepare to compute results
shipmode_counts = {}

# Process lineitem and merge with order priorities from MySQL
for lineitem in lineitems:
    l_orderkey = lineitem['L_ORDERKEY']
    if l_orderkey in order_priorities:
        shipmode = lineitem['L_SHIPMODE']
        priority = order_priorities[l_orderkey]

        if shipmode not in shipmode_counts:
            shipmode_counts[shipmode] = {'HIGH_LINE_COUNT': 0, 'LOW_LINE_COUNT': 0}

        if priority in ('1-URGENT', '2-HIGH'):
            shipmode_counts[shipmode]['HIGH_LINE_COUNT'] += 1
        else:
            shipmode_counts[shipmode]['LOW_LINE_COUNT'] += 1

# Write query results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for shipmode, counts in sorted(shipmode_counts.items()):
        writer.writerow({
            'L_SHIPMODE': shipmode,
            'HIGH_LINE_COUNT': counts['HIGH_LINE_COUNT'],
            'LOW_LINE_COUNT': counts['LOW_LINE_COUNT']
        })

# Close MongoDB connection
mongodb_client.close()
