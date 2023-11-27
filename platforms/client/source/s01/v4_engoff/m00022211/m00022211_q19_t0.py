import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_lineitem = mongo_db['lineitem']

# MySQL query
mysql_query = """
SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER
FROM part
WHERE (
          (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
       OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
       OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
      )
"""

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    part_results = {row[0]: row for row in cursor.fetchall()}

# Filtering documents in MongoDB
mongo_query = {
    'L_SHIPMODE': { '$in': ['AIR', 'AIR REG'] },
    '$or': [
        { 'L_QUANTITY': { '$gte': 1, '$lte': 11 }, 'L_PARTKEY': { '$in': list(part_results.keys()) } },
        { 'L_QUANTITY': { '$gte': 10, '$lte': 20 }, 'L_PARTKEY': { '$in': list(part_results.keys()) } },
        { 'L_QUANTITY': { '$gte': 20, '$lte': 30 }, 'L_PARTKEY': { '$in': list(part_results.keys()) } },
    ]
}
mongo_lineitems = list(mongo_lineitem.find(mongo_query))

# Combine results and write to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['P_PARTKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'L_ORDERKEY', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])
    
    for lineitem in mongo_lineitems:
        part_info = part_results.get(lineitem['L_PARTKEY'])
        if part_info:
            writer.writerow([
                part_info[0],
                part_info[1],
                part_info[2],
                part_info[3],
                part_info[4],
                lineitem['L_ORDERKEY'],
                lineitem['L_QUANTITY'],
                lineitem['L_EXTENDEDPRICE'],
                lineitem['L_DISCOUNT']
            ])

# Close connections
mysql_conn.close()
mongo_client.close()
