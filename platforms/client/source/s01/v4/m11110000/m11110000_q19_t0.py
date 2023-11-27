import csv
import pymysql
from pymongo import MongoClient

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
part_collection = mongodb['part']

# Get all parts with required conditions from MongoDB
query = {'$or': [
    {'P_BRAND': 'Brand#12', 'P_CONTAINER': {'$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']}, 'P_SIZE': {'$gte': 1, '$lte': 5}},
    {'P_BRAND': 'Brand#23', 'P_CONTAINER': {'$in': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']}, 'P_SIZE': {'$gte': 1, '$lte': 10}},
    {'P_BRAND': 'Brand#34', 'P_CONTAINER': {'$in': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']}, 'P_SIZE': {'$gte': 1, '$lte': 15}}
]}

part_data = part_collection.find(query, {'P_PARTKEY': 1})
part_keys = [part['P_PARTKEY'] for part in part_data]

# Query for the lineitem table in MySQL
lineitem_sql = """
SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM lineitem
WHERE
    (
        L_PARTKEY IN (%s)
        AND L_QUANTITY >= 1 AND L_QUANTITY <= 11
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    )
    OR
    (
        L_PARTKEY IN (%s)
        AND L_QUANTITY >= 10 AND L_QUANTITY <= 20
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    )
    OR
    (
        L_PARTKEY IN (%s)
        AND L_QUANTITY >= 20 AND L_QUANTITY <= 30
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    )
"""

# Adjust the query for the number of part keys
formatted_sql = lineitem_sql % (','.join(['%s'] * len(part_keys)), ','.join(['%s'] * len(part_keys)), ','.join(['%s'] * len(part_keys)))

# Execute the query
with mysql_conn.cursor() as cursor:
    cursor.execute(formatted_sql, part_keys * 3)
    result = cursor.fetchone()

# Write the output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['REVENUE'])
    csvwriter.writerow([result[0] if result else ''])

# Close connections
mysql_conn.close()
mongo_client.close()
