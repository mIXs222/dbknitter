import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Retrieve parts that match the criteria from MongoDB
brand_containers = {
    'Brand#12': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'],
    'Brand#23': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'],
    'Brand#34': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']
}

# Extract matching part keys from MongoDB
part_keys = {}
for brand, containers in brand_containers.items():
    for p_size in brand_containers[brand]:
        mongo_query = {
            'P_BRAND': brand,
            'P_CONTAINER': {'$in': containers},
            'P_SIZE': {'$gte': 1, '$lte': 15}
        }
        parts_cursor = part_collection.find(mongo_query, {'P_PARTKEY': 1})
        for doc in parts_cursor:
            part_keys[doc["P_PARTKEY"]] = True

# Close MongoDB client
mongo_client.close()

# Build the MySQL query statement with the retrieved part keys
part_keys_str = ','.join(str(k) for k in part_keys.keys())
mysql_query = f"""
SELECT
  SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM
  lineitem
WHERE
  L_PARTKEY IN ({part_keys_str})
  AND ((L_QUANTITY >= 1 AND L_QUANTITY <= 11 AND L_PARTKEY IN (
       SELECT P_PARTKEY FROM part WHERE P_BRAND = 'Brand#12'))
       OR (L_QUANTITY >= 10 AND L_QUANTITY <= 20 AND L_PARTKEY IN (
       SELECT P_PARTKEY FROM part WHERE P_BRAND = 'Brand#23'))
       OR (L_QUANTITY >= 20 AND L_QUANTITY <= 30 AND L_PARTKEY IN (
       SELECT P_PARTKEY FROM part WHERE P_BRAND = 'Brand#34')))
  AND L_SHIPMODE IN ('AIR', 'AIR REG')
  AND L_SHIPINSTRUCT = 'DELIVER IN PERSON';
"""

# Execute MySQL query and output to CSV
mysql_cursor.execute(mysql_query)
result = mysql_cursor.fetchone()

# Write query result to csv file
with open('query_output.csv', 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['REVENUE'])
    writer.writerow(result)

# Close MySQL connections
mysql_cursor.close()
mysql_conn.close()
