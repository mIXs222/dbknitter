import pymysql
import pymongo
import csv

# Connect to MySQL server
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB server
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Fetch orders data from MySQL
orders_query = """
SELECT O_ORDERKEY, O_ORDERPRIORITY
FROM orders
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(orders_query)
    orders_data = cursor.fetchall()

# Create a dictionary for orders with O_ORDERKEY as key
orders_dict = {order[0]: order[1] for order in orders_data}

# Query MongoDB for lineitem data with the provided filters
lineitem_cursor = lineitem_collection.find({
    'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
    'L_COMMITDATE': {'$lt': '1995-01-01'},
    'L_SHIPDATE': {'$lt': "$L_COMMITDATE"},
    'L_RECEIPTDATE': {'$gte': '1994-01-01'}
})

# Define our aggregation buckets
ship_mode_stats = {}

# Process lineitem records and compute high/low line count based on the orders data
for lineitem in lineitem_cursor:
    orderkey = lineitem['L_ORDERKEY']
    orderpriority = orders_dict.get(orderkey, None)
    
    if orderpriority is None:
        continue
    
    # Initialize counters for each SHIPMODE
    ship_mode = lineitem['L_SHIPMODE']
    if ship_mode not in ship_mode_stats:
        ship_mode_stats[ship_mode] = {'HIGH_LINE_COUNT': 0, 'LOW_LINE_COUNT': 0}

    if orderpriority in ('1-URGENT', '2-HIGH'):
        ship_mode_stats[ship_mode]['HIGH_LINE_COUNT'] += 1
    else:
        ship_mode_stats[ship_mode]['LOW_LINE_COUNT'] += 1

# Close the database connections
mysql_conn.close()
mongo_client.close()

# Write query results to file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for ship_mode, counts in sorted(ship_mode_stats.items()):
        writer.writerow({'L_SHIPMODE': ship_mode, 'HIGH_LINE_COUNT': counts['HIGH_LINE_COUNT'], 'LOW_LINE_COUNT': counts['LOW_LINE_COUNT']})
