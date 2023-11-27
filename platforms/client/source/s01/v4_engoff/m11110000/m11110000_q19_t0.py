import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_part = mongo_db['part']

# Query definitions for MongoDB
brand_container_mapping = {
    12: ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'],
    23: ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'],
    34: ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']
}

# Extracting parts from MongoDB
partkeys = set()
for brand_id, containers in brand_container_mapping.items():
   for container in containers:
        size_range = {
            12: {'$gte': 1, '$lte': 5},
            23: {'$gte': 1, '$lte': 10},
            34: {'$gte': 1, '$lte': 15}
        }[brand_id]
        query = {
            'P_BRAND': f'Brand#{brand_id}',
            'P_CONTAINER': container,
            'P_SIZE': size_range
        }
        parts_cursor = mongo_part.find(query, {'P_PARTKEY': 1})
        partkeys.update([part['P_PARTKEY'] for part in parts_cursor])

# Prepare SQL query
quantity_ranges = {
    'SM': (1, 11),
    'MED.': (10, 20),
    'LG': (20, 30)
}
shipmodes = ['AIR', 'AIR REG']
sql_query = """
SELECT
    L_ORDERKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as REVENUE
FROM
    lineitem
WHERE
    L_PARTKEY IN ({}) AND
    L_SHIPINSTRUCT = 'DELIVER IN PERSON' AND
    L_SHIPMODE IN ('{}', '{}') AND
    L_QUANTITY BETWEEN {} AND {}
GROUP BY L_ORDERKEY;
"""

# Fetching results from MySQL and write to CSV
output = []
for size_prefix, quantity_range in quantity_ranges.items():
    formatted_query = sql_query.format(
        ', '.join(str(pk) for pk in partkeys),
        *shipmodes,
        *quantity_range
    )
    mysql_cursor.execute(formatted_query)
    results = mysql_cursor.fetchall()
    output.extend(results)

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['L_ORDERKEY', 'REVENUE'])
    for row in output:
        csv_writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
