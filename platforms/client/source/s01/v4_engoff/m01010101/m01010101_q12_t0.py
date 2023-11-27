import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Get orders with URGENT or HIGH priority from MySQL
query_urgent_high_priority = """
SELECT O_ORDERKEY, O_ORDERPRIORITY FROM orders
WHERE O_ORDERPRIORITY IN ('URGENT', 'HIGH')
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(query_urgent_high_priority)
    urgent_high_priority_orders = set(row[0] for row in cursor)

# Execute MongoDB query for lineitem
query_lineitem = {
    'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
    'L_RECEIPTDATE': {'$gte': datetime(1994, 1, 1), '$lt': datetime(1995, 1, 1)},
    '$expr': {'$gt': ['$L_RECEIPTDATE', '$L_COMMITDATE']},
    'L_SHIPDATE': {'$lt': '$L_COMMITDATE'}
}

late_lineitems = list(mongodb_db.lineitem.find(query_lineitem, projection={'L_ORDERKEY': 1, 'L_SHIPMODE': 1}))

# Process the results
results = {
    'MAIL': {'URGENT_HIGH': 0, 'OTHER': 0},
    'SHIP': {'URGENT_HIGH': 0, 'OTHER': 0}
}

for lineitem in late_lineitems:
    priority_group = 'URGENT_HIGH' if lineitem['L_ORDERKEY'] in urgent_high_priority_orders else 'OTHER'
    results[lineitem['L_SHIPMODE']][priority_group] += 1

# Write the results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['SHIP_MODE', 'PRIORITY_GROUP', 'LATE_LINEITEMS']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for ship_mode in results:
        for priority_group in results[ship_mode]:
            writer.writerow({
                'SHIP_MODE': ship_mode,
                'PRIORITY_GROUP': priority_group,
                'LATE_LINEITEMS': results[ship_mode][priority_group]
            })

# Close the connections
mysql_conn.close()
mongodb_client.close()
