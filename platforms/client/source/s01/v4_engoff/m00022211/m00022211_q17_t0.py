import pymysql
import pymongo
import csv
from datetime import datetime
from decimal import Decimal

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# MySQL Query to fetch parts information
mysql_query = """
SELECT P_PARTKEY, P_BRAND, P_CONTAINER
FROM part
WHERE (P_BRAND = 'Brand#23') AND (P_CONTAINER = 'MED BAG');
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    qualifying_parts = cursor.fetchall()

# Extract part keys from MySQL results
qualifying_part_keys = [row[0] for row in qualifying_parts]

# MongoDB Query to calculate average quantity for qualifying parts
pipeline = [
    {"$match": {
        'L_PARTKEY': {'$in': qualifying_part_keys}
    }},
    {"$group": {
        '_id': None,
        'average_quantity': {'$avg': '$L_QUANTITY'}
    }}
]

average_quantity_result = mongo_db.lineitem.aggregate(pipeline)
average_quantity = None
for result in average_quantity_result:
    average_quantity = result['average_quantity']

# MongoDB Query to calculate potential revenue loss
pipeline = [
    {"$match": {
        'L_PARTKEY': {'$in': qualifying_part_keys},
        'L_QUANTITY': {'$lt': average_quantity * 0.2}
    }},
    {"$group": {
        '_id': None,
        'total_revenue_loss': {'$sum': '$L_EXTENDEDPRICE'}
    }}
]
loss_result = mongo_db.lineitem.aggregate(pipeline)
loss = None
for result in loss_result:
    loss = result['total_revenue_loss']

# Close the connections
mysql_conn.close()
mongo_client.close()

# Calculate the average yearly gross loss
years = 7
average_yearly_loss = (Decimal(loss) / years).quantize(Decimal('.01'))

# Write the output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['average_yearly_loss'])
    writer.writerow([str(average_yearly_loss)])
