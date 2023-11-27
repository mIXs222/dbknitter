import csv
import pymysql
from pymongo import MongoClient

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch
lineitem_collection = mongodb.lineitem

# Fetch the parts from MySQL that match the conditions
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(
    "SELECT P_PARTKEY FROM part "
    "WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'"
)
valid_parts = mysql_cursor.fetchall()
valid_partkeys = [part[0] for part in valid_parts]

# Calculate the average quantity for each part in MongoDB
avg_quantity = {}
for partkey in valid_partkeys:
    avg_q = list(lineitem_collection.aggregate([
        {"$match": {"L_PARTKEY": partkey}},
        {"$group": {"_id": "$L_PARTKEY", "avg_quantity": {"$avg": "$L_QUANTITY"}}}
    ]))
    if avg_q:
        avg_quantity[partkey] = avg_q[0]['avg_quantity']

# Fetch the lineitems from MongoDB that match the conditions
lineitem_cursor = lineitem_collection.find({
    "L_PARTKEY": {"$in": valid_partkeys},
    "L_QUANTITY": {"$lt": 0.2 * avg_quantity[partkey]}
})

total_extended_price = sum([lineitem['L_EXTENDEDPRICE'] for lineitem in lineitem_cursor])

# Calculate the result
avg_yearly = total_extended_price / 7.0

# Write the result to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['AVG_YEARLY'])
    writer.writerow([avg_yearly])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
