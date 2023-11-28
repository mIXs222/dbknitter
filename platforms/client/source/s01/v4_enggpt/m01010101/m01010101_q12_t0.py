import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

# Prepare the query for the 'orders' table in MySQL
order_query = """
SELECT O_ORDERKEY, O_ORDERPRIORITY FROM orders
WHERE O_ORDERPRIORITY IN ('1-URGENT', '2-HIGH') OR O_ORDERPRIORITY NOT IN ('1-URGENT', '2-HIGH')
"""

# Execute the query and fetch the results
with mysql_conn.cursor() as cursor:
    cursor.execute(order_query)
    orders_priority = cursor.fetchall()  # Fetch all order priorities

# Convert orders data to a dictionary for quick lookup
order_priority_dict = {row[0]: row[1] for row in orders_priority}

# Filter 'lineitem' collection in MongoDB and project necessary fields
start_date = datetime(1994, 1, 1)
end_date = datetime(1994, 12, 31)
lineitems_cursor = lineitem_collection.find({
    'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
    'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'},
    'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
    'L_RECEIPTDATE': {'$gte': start_date, '$lte': end_date}
}, {
    'L_ORDERKEY': 1, 'L_SHIPMODE': 1, '_id': 0
})

# Iterate over line items and perform the analysis
shipping_mode_counts = {}
for lineitem in lineitems_cursor:
    order_key = lineitem['L_ORDERKEY']
    ship_mode = lineitem['L_SHIPMODE']

    if order_key in order_priority_dict:
        order_priority = order_priority_dict[order_key]
        if ship_mode not in shipping_mode_counts:
            shipping_mode_counts[ship_mode] = {'HIGH_LINE_COUNT': 0, 'LOW_LINE_COUNT': 0}

        if order_priority in ('1-URGENT', '2-HIGH'):
            shipping_mode_counts[ship_mode]['HIGH_LINE_COUNT'] += 1
        else:
            shipping_mode_counts[ship_mode]['LOW_LINE_COUNT'] += 1

# Writing query results to output file
output_file = 'query_output.csv'
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write headers
    writer.writerow(['SHIPPING_MODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
    # Write data rows, ordered by shipping mode
    for ship_mode in sorted(shipping_mode_counts):
        writer.writerow([ship_mode, shipping_mode_counts[ship_mode]['HIGH_LINE_COUNT'],
                         shipping_mode_counts[ship_mode]['LOW_LINE_COUNT']])

# Close the database connections
mysql_conn.close()
mongodb_client.close()
