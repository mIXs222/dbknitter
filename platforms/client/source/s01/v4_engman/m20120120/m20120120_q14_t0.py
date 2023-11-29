# query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['part']

# Get parts that are promotional from MongoDB
promotional_parts = list(mongo_collection.find(
    {"P_TYPE": {"$regex": "PROMO"}},
    {"_id": 0, "P_PARTKEY": 1}
))

promotional_part_keys = set([p['P_PARTKEY'] for p in promotional_parts])

# Query to MySQL to get revenue from lineitem table
mysql_query = """
SELECT
    L_PARTKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE < '1995-10-01'
"""

mysql_cursor.execute(mysql_query)

total_revenue = 0.0
promo_revenue = 0.0

# Calculate revenues
for row in mysql_cursor:
    part_key, extended_price, discount = row
    revenue = extended_price * (1 - discount)
    total_revenue += revenue
    if part_key in promotional_part_keys:
        promo_revenue += revenue

mysql_cursor.close()
mysql_conn.close()
mongo_client.close()

# Calculate and write the results
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0
results = [
    ["PROMO_REVENUE", promo_revenue],
    ["TOTAL_REVENUE", total_revenue],
    ["PROMO_PERCENTAGE", promo_revenue_percentage],
]

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["TYPE", "VALUE"])
    for result in results:
        writer.writerow(result)
