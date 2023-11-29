import pymysql
import pymongo
import csv

# Connecting to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Connecting to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client['tpch']
part_collection = mongodb_db['part']

# Querying MongoDB
part_query = {
    '$or': [
        {'$and': [
            {'P_BRAND': 'Brand#12'},
            {'P_CONTAINER': {'$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']}},
            {'P_SIZE': {'$gte': 1, '$lte': 5}}
        ]},
        {'$and': [
            {'P_BRAND': 'Brand#23'},
            {'P_CONTAINER': {'$in': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']}},
            {'P_SIZE': {'$gte': 1, '$lte': 10}}
        ]},
        {'$and': [
            {'P_BRAND': 'Brand#34'},
            {'P_CONTAINER': {'$in': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']}},
            {'P_SIZE': {'$gte': 1, '$lte': 15}}
        ]}
    ]
}
parts_cursor = part_collection.find(part_query)
part_keys = [part['P_PARTKEY'] for part in parts_cursor]

# Querying MySQL
mysql_cursor = mysql_conn.cursor()

mysql_query = """
SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM lineitem
WHERE L_PARTKEY IN (%s)
AND L_SHIPMODE IN ('AIR', 'AIR REG')
AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
AND (
    (L_QUANTITY >= 1 AND L_QUANTITY <= 11 AND L_PARTKEY in (
        SELECT P_PARTKEY FROM part WHERE P_BRAND = 'Brand#12' AND P_CONTAINER in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5))
OR
    (L_QUANTITY >= 10 AND L_QUANTITY <= 20 AND L_PARTKEY in (
        SELECT P_PARTKEY FROM part WHERE P_BRAND = 'Brand#23' AND P_CONTAINER in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10))
OR
    (L_QUANTITY >= 20 AND L_QUANTITY <= 30 AND L_PARTKEY in (
        SELECT P_PARTKEY FROM part WHERE P_BRAND = 'Brand#34' AND P_CONTAINER in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15))
)
"""

# Formatting query for IN clause
formatted_query = mysql_query % ','.join(['%s'] * len(part_keys))

mysql_cursor.execute(formatted_query, part_keys)

# Writing the result to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['REVENUE'])
    for row in mysql_cursor:
        csvwriter.writerow(row)

# Clean up
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
