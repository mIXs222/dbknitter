import csv
import pymysql
import pymongo

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client['tpch']
lineitem_collection = mongodb['lineitem']

# MySQL queries by part type
mysql_queries = {
    1: """
        SELECT p.P_PARTKEY
        FROM part p
        WHERE p.P_BRAND = 'Brand#12'
        AND p.P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND p.P_SIZE BETWEEN 1 AND 5
    """,
    2: """
        SELECT p.P_PARTKEY
        FROM part p
        WHERE p.P_BRAND = 'Brand#23'
        AND p.P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND p.P_SIZE BETWEEN 1 AND 10
    """,
    3: """
        SELECT p.P_PARTKEY
        FROM part p
        WHERE p.P_BRAND = 'Brand#34'
        AND p.P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND p.P_SIZE BETWEEN 1 AND 15
    """
}

# Get part keys for all conditions from MySQL
part_keys = {1: [], 2: [], 3: []}
with mysql_conn.cursor() as cursor:
    for type_id, query in mysql_queries.items():
        cursor.execute(query)
        part_keys[type_id] = [row[0] for row in cursor.fetchall()]

# MongoDB queries by part type
mongodb_queries = {
    1: {
        'L_QUANTITY': {'$gte': 1, '$lte': 11},
        'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
        'L_SHIPINSTRUCT': 'DELIVER IN PERSON',
        'L_PARTKEY': {'$in': part_keys[1]}
    },
    2: {
        'L_QUANTITY': {'$gte': 10, '$lte': 20},
        'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
        'L_SHIPINSTRUCT': 'DELIVER IN PERSON',
        'L_PARTKEY': {'$in': part_keys[2]}
    },
    3: {
        'L_QUANTITY': {'$gte': 20, '$lte': 30},
        'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
        'L_SHIPINSTRUCT': 'DELIVER IN PERSON',
        'L_PARTKEY': {'$in': part_keys[3]}
    }
}

# Calculate revenue from MongoDB
total_revenue = 0
for query in mongodb_queries.values():
    for document in lineitem_collection.find(query):
        total_revenue += document['L_EXTENDEDPRICE'] * (1 - document['L_DISCOUNT'])

# Write the result to csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['REVENUE'])
    writer.writerow([total_revenue])

# Close connections
mysql_conn.close()
mongo_client.close()
