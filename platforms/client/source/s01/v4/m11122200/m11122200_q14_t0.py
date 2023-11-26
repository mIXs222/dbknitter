import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Get the part data
promo_parts = list(part_collection.find({"P_TYPE": {"$regex": "^PROMO"}}, {"P_PARTKEY": 1}))

# Extract part keys
promo_partkeys = {p['P_PARTKEY'] for p in promo_parts}

# Query MySQL for lineitem data
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("""
    SELECT
        L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
    FROM
        lineitem
    WHERE
        L_SHIPDATE >= '1995-09-01'
    AND
        L_SHIPDATE < '1995-10-01'
""")

# Processing the result
promo_sum = 0.0
total_sum = 0.0
for row in mysql_cursor:
    if row[0] in promo_partkeys:
        promo_sum += row[1] * (1 - row[2])
    total_sum += row[1] * (1 - row[2])

# Compute the final result if total sum is not zero; otherwise, assign zero.
promo_revenue = (100.0 * promo_sum / total_sum) if total_sum != 0 else 0

# Write the output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PROMO_REVENUE'])
    writer.writerow([promo_revenue])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
