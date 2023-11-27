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
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_lineitem = mongo_db['lineitem']

# Execute query for MySQL to fetch orders with URGENT or HIGH priority
mysql_query = """
SELECT O_ORDERKEY, O_ORDERPRIORITY 
FROM orders 
WHERE O_ORDERPRIORITY = 'URGENT' OR O_ORDERPRIORITY = 'HIGH'
"""
mysql_cursor.execute(mysql_query)
urgent_high_orders = dict((row[0], row[1]) for row in mysql_cursor.fetchall())

# Query MongoDB for lineitems
late_lineitems = mongo_lineitem.aggregate([
    {
        '$match': {
            'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
            'L_RECEIPTDATE': {
                '$gte': datetime(1994, 1, 1), 
                '$lt': datetime(1995, 1, 1)
            },
            'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
        }
    },
    {
        '$addFields': {
            'is_late': {'$gt': ['$L_RECEIPTDATE', '$L_COMMITDATE']}
        }
    }
])

# Process MongoDB results and count late lineitems by ship mode and priority
counts = {
    'MAIL': {'URGENT/HIGH': 0, 'OTHER': 0},
    'SHIP': {'URGENT/HIGH': 0, 'OTHER': 0}
}

for lineitem in late_lineitems:
    if not lineitem['is_late']:
        continue
    order_key = lineitem['L_ORDERKEY']
    ship_mode = lineitem['L_SHIPMODE']
    priority = 'URGENT/HIGH' if order_key in urgent_high_orders else 'OTHER'
    counts[ship_mode][priority] += 1

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['SHIPMODE', 'PRIORITY', 'LATE_LINEITEMS_COUNT'])
    for ship_mode in counts:
        for priority in counts[ship_mode]:
            writer.writerow([ship_mode, priority, counts[ship_mode][priority]])
