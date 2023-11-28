import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

# Fetch high and low priority orders from MySQL
high_priority_orders = []
low_priority_orders = []
try:
    mysql_cursor.execute(
        "SELECT O_ORDERKEY, O_ORDERPRIORITY FROM orders "
        "WHERE O_ORDERPRIORITY IN ('1-URGENT', '2-HIGH')"
    )
    high_priority_orders = mysql_cursor.fetchall()
    
    mysql_cursor.execute(
        "SELECT O_ORDERKEY FROM orders WHERE O_ORDERPRIORITY NOT IN ('1-URGENT', '2-HIGH')"
    )
    low_priority_orders = mysql_cursor.fetchall()
finally:
    mysql_cursor.close()
    mysql_conn.close()

high_priority_orders_keys = {row[0] for row in high_priority_orders}
low_priority_orders_keys = {row[0] for row in low_priority_orders}

# Convert string dates to datetime objects for comparison
start_date = datetime.strptime('1994-01-01', '%Y-%m-%d')
end_date = datetime.strptime('1994-12-31', '%Y-%m-%d')

# Process the lineitems in MongoDB
results = {'MAIL': {'HIGH_LINE_COUNT': 0, 'LOW_LINE_COUNT': 0},
           'SHIP': {'HIGH_LINE_COUNT': 0, 'LOW_LINE_COUNT': 0}}

for lineitem in lineitem_collection.find():
    if (lineitem['L_SHIPMODE'] in ['MAIL', 'SHIP'] and
        lineitem['L_COMMITDATE'] < lineitem['L_RECEIPTDATE'] and
        lineitem['L_SHIPDATE'] < lineitem['L_COMMITDATE'] and
        start_date <= lineitem['L_RECEIPTDATE'] <= end_date):

        if lineitem['L_ORDERKEY'] in high_priority_orders_keys:
            results[lineitem['L_SHIPMODE']]['HIGH_LINE_COUNT'] += 1
        elif lineitem['L_ORDERKEY'] in low_priority_orders_keys:
            results[lineitem['L_SHIPMODE']]['LOW_LINE_COUNT'] += 1

# Write the results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['SHIPPING_MODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])

    for mode in sorted(results.keys()):
        writer.writerow([mode, results[mode]['HIGH_LINE_COUNT'], results[mode]['LOW_LINE_COUNT']])
