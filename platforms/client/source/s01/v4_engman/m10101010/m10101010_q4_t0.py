# query.py

import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# MySQL Query
mysql_query = """
SELECT 
    L_ORDERKEY 
FROM 
    lineitem 
WHERE 
    L_COMMITDATE < L_RECEIPTDATE
"""

try:
    with mysql_connection.cursor() as cursor:
        cursor.execute(mysql_query)
        # Fetch all the qualifying lineitem order keys
        lineitem_orders = cursor.fetchall()
except Exception as e:
    print(f"Error fetching data from MySQL: {e}")
finally:
    mysql_connection.close()

# List of qualifying order keys from the lineitem table
qualifying_order_keys = [row[0] for row in lineitem_orders]

# MongoDB query
mongo_pipeline = [
    {
        '$match': {
            'O_ORDERDATE': {'$gte': '1993-07-01', '$lt': '1993-10-01'},
            'O_ORDERKEY': {'$in': qualifying_order_keys}
        }
    },
    {
        '$group': {
            '_id': '$O_ORDERPRIORITY',
            'ORDER_COUNT': {'$count': {}}
        }
    },
    {
        '$sort': {'_id': 1}
    },
    {
        '$project': {
            'O_ORDERPRIORITY': '$_id',
            'ORDER_COUNT': 1,
            '_id': 0
        }
    }
]

try:
    order_counts = orders_collection.aggregate(mongo_pipeline)
    order_counts = list(order_counts)
except Exception as e:
    print(f"Error fetching data from MongoDB: {e}")

# Write to CSV
with open("query_output.csv", "w", newline='') as csvfile:
    fieldnames = ['ORDER_COUNT', 'O_ORDERPRIORITY']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for count in order_counts:
        writer.writerow({'ORDER_COUNT': count['ORDER_COUNT'], 'O_ORDERPRIORITY': count['O_ORDERPRIORITY']})
