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
orders_collection = mongodb['orders']

# Function to execute MySQL query
def execute_mysql_query(query):
    with mysql_conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

# Function to format order keys for Mongo query
def format_order_keys(order_keys):
    return [{"O_ORDERKEY": key} for key in order_keys]

# MySQL query for lineitem
mysql_query_lineitem = """
SELECT L_ORDERKEY, L_SHIPMODE, L_COMMITDATE, L_SHIPDATE, L_RECEIPTDATE
FROM lineitem
WHERE L_SHIPMODE IN ('MAIL', 'SHIP')
AND L_COMMITDATE < L_RECEIPTDATE
AND L_SHIPDATE < L_COMMITDATE
AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1994-12-31';
"""

# Execute the query and fetch lineitems
lineitems = execute_mysql_query(mysql_query_lineitem)
order_keys_of_lineitems = [lineitem[0] for lineitem in lineitems]

# MongoDB query to fetch filtered orders
filtered_orders = orders_collection.find(
    {"$or": format_order_keys(order_keys_of_lineitems)}
)

# Create a dictionary for order priority lookup
order_priority_lookup = {order['O_ORDERKEY']: order['O_ORDERPRIORITY'] for order in filtered_orders}

# Process results to classify HIGH and LOW priority counts
shipping_mode_count = {}
for lineitem in lineitems:
    order_key = lineitem[0]
    ship_mode = lineitem[1]
    order_priority = order_priority_lookup.get(order_key, None)
    
    if order_priority in ('1-URGENT', '2-HIGH'):
        priority_type = 'HIGH_LINE_COUNT'
    else:
        priority_type = 'LOW_LINE_COUNT'

    if ship_mode not in shipping_mode_count:
        shipping_mode_count[ship_mode] = {'HIGH_LINE_COUNT': 0, 'LOW_LINE_COUNT': 0}

    shipping_mode_count[ship_mode][priority_type] += 1

# Write the result to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['SHIPPING_MODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])

    for ship_mode, counts in sorted(shipping_mode_count.items()):
        writer.writerow([ship_mode, counts['HIGH_LINE_COUNT'], counts['LOW_LINE_COUNT']])

# Close the connections
mysql_conn.close()
mongo_client.close()

print("The query_output.csv file has been created.")
