# query.py

import csv
import pymysql
import pymongo
from datetime import datetime

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
lineitem_collection = mongodb['lineitem']

# Define the dates for filtering
start_date = datetime.strptime('1995-09-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-10-01', '%Y-%m-%d')

# Get relevant 'lineitem' documents from MongoDB
lineitems = list(lineitem_collection.find({
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
}, {'L_PARTKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1}))

# Map L_PARTKEY to the required fields from lineitems
lineitem_map = {item['L_PARTKEY']: item for item in lineitems}

# Prepare the MySQL query
mysql_query = """
SELECT P_PARTKEY, P_TYPE, P_RETAILPRICE
FROM part
WHERE P_PARTKEY IN (%s)
"""

# Execute the MySQL query with the list of part keys from MongoDB documents
part_keys = list(lineitem_map.keys())
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(mysql_query, part_keys)

promo_revenue_numerator = 0.0
promo_revenue_denominator = 0.0

# Calculate required fields for the query
for part in mysql_cursor:
    part_key, p_type, p_retailprice = part
    if part_key not in lineitem_map:
        continue
    lineitem = lineitem_map[part_key]
    extended_price = lineitem['L_EXTENDEDPRICE']
    discount = lineitem['L_DISCOUNT']
    
    promo_revenue_denominator += extended_price * (1 - discount)
    
    if p_type.startswith('PROMO'):
        promo_revenue_numerator += extended_price * (1 - discount)

# Close MySQL cursor and connection
mysql_cursor.close()
mysql_conn.close()

# Calculate the PROMO_REVENUE
if promo_revenue_denominator != 0:
    promo_revenue = 100.00 * (promo_revenue_numerator / promo_revenue_denominator)
else:
    promo_revenue = None

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['PROMO_REVENUE'])
    writer.writerow([promo_revenue])

print("Query finished and output is written to query_output.csv")
