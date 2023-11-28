# script.py

import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

# Fetch parts data from MySQL
mysql_query = """
SELECT
    P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE
FROM
    part
WHERE
    (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5) OR
    (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10) OR
    (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""
mysql_cursor.execute(mysql_query)
parts_tuples = mysql_cursor.fetchall()

# Define a function to perform the MongoDB query
def fetch_lineitems(part_key, quantity_range_min, quantity_range_max):
    lineitem_documents = lineitem_collection.find({
        'L_PARTKEY': part_key,
        'L_QUANTITY': {'$gte': quantity_range_min, '$lte': quantity_range_max},
        'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
        'L_SHIPINSTRUCT': 'DELIVER IN PERSON'
    })
    return list(lineitem_documents)

# Perform revenue analysis
revenue_data = []

for part in parts_tuples:
    part_key = part[0]
    if 'Brand#12' == part[3]:
        quantity_range = (1, 11)
    elif 'Brand#23' == part[3]:
        quantity_range = (10, 20)
    elif 'Brand#34' == part[3]:
        quantity_range = (20, 30)
    else:
        continue
    
    lineitems = fetch_lineitems(part_key, *quantity_range)
    
    for lineitem in lineitems:
        revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
        revenue_data.append((lineitem['L_ORDERKEY'], part_key, revenue))

# Writing revenue data to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['L_ORDERKEY', 'P_PARTKEY', 'REVENUE'])
    csv_writer.writerows(revenue_data)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
