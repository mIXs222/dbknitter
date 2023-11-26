import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Pull part data from MongoDB that matches the condition
promo_parts = part_collection.find({"P_TYPE": {"$regex": r'^PROMO'}})
promo_parts_dict = {doc['P_PARTKEY']: doc for doc in promo_parts}

# Query MySQL for lineitems
with mysql_connection.cursor() as cursor:
    query = """
    SELECT
        L_PARTKEY,
        L_EXTENDEDPRICE,
        L_DISCOUNT
    FROM
        lineitem
    WHERE
        L_SHIPDATE >= '1995-09-01'
        AND L_SHIPDATE < '1995-10-01'
    """
    cursor.execute(query)
    lineitems = cursor.fetchall()

# Calculate the promo_revenue
promo_revenue_numerator = 0.0
promo_revenue_denominator = 0.0

for item in lineitems:
    extended_price = item[1]
    discount = item[2]
    if item[0] in promo_parts_dict:
        promo_revenue_numerator += extended_price * (1 - discount)
    promo_revenue_denominator += extended_price * (1 - discount)

# Safeguard against division by zero
if promo_revenue_denominator == 0:
    promo_revenue = 0
else:
    promo_revenue = 100.00 * promo_revenue_numerator / promo_revenue_denominator

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['PROMO_REVENUE'])
    writer.writerow([promo_revenue])

# Close connections
mysql_connection.close()
mongo_client.close()
