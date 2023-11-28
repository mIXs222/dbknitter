import csv
import pymysql
import pymongo
from datetime import datetime

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_connection.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']

# SQL query to get valid order keys with conditions met
mysql_query = """
SELECT DISTINCT L_ORDERKEY
FROM lineitem
WHERE L_COMMITDATE < L_RECEIPTDATE
AND L_SHIPDATE BETWEEN '1993-07-01' AND '1993-10-01';
"""

# Execute the MySQL query
mysql_cursor.execute(mysql_query)
valid_order_keys = [row[0] for row in mysql_cursor.fetchall()]

# Date format for MongoDB
date_format = '%Y-%m-%d'

# Query MongoDB for the orders meeting the specified order keys
mongodb_result = orders_collection.aggregate([
    {'$match': {
        'O_ORDERDATE': {
            '$gte': datetime.strptime('1993-07-01', date_format),
            '$lte': datetime.strptime('1993-10-01', date_format)
        },
        'O_ORDERKEY': {'$in': valid_order_keys}
    }},
    {'$group': {
        '_id': '$O_ORDERPRIORITY',
        'count': {'$sum': 1}
    }},
    {'$sort': {'_id': 1}}
])

# Prepare the result
query_output = [['Order Priority', 'Count']]
query_output.extend([[result['_id'], result['count']] for result in mongodb_result])

# Write to CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(query_output)

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
