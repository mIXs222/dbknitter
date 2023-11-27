# query.py
import pymysql
import pymongo
import csv

# MongoDB connection parameters
mongo_host = 'mongodb'
mongo_port = 27017
mongo_db = 'tpch'

# MySQL connection parameters
mysql_host = 'mysql'
mysql_user = 'root'
mysql_password = 'my-secret-pw'
mysql_db = 'tpch'

# Connect to MongoDB
mongo_client = pymongo.MongoClient(mongo_host, mongo_port)
mongo_database = mongo_client[mongo_db]
part_collection = mongo_database['part']

# MongoDB query
mongo_query = {
    "$or": [
        {
            "P_BRAND": "Brand#12",
            "P_CONTAINER": {"$in": ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']},
            "P_SIZE": {"$gte": 1, "$lte": 5}
        },
        {
            "P_BRAND": "Brand#23",
            "P_CONTAINER": {"$in": ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']},
            "P_SIZE": {"$gte": 1, "$lte": 10}
        },
        {
            "P_BRAND": "Brand#34",
            "P_CONTAINER": {"$in": ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']},
            "P_SIZE": {"$gte": 1, "$lte": 15}
        }
    ]
}

# Execute MongoDB query
part_keys = {doc['P_PARTKEY']: doc for doc in part_collection.find(mongo_query)}

# Connect to MySQL
mysql_connection = pymysql.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    database=mysql_db
)

# Prepare MySQL query
mysql_query = '''
    SELECT 
        L_ORDERKEY, L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT
    FROM 
        lineitem
    WHERE 
        L_PARTKEY IN (%s) AND
        L_QUANTITY >= 1 AND L_QUANTITY <= 31 AND
        L_SHIPMODE IN ('AIR', 'AIR REG') AND
        L_SHIPINSTRUCT = 'DELIVER IN PERSON'
'''

# Iterate over the matched part keys to get the L_PARTKEY placeholders
placeholders = ', '.join(['%s'] * len(part_keys))
mysql_query_formatted = mysql_query % placeholders

# Execute MySQL query
cur = mysql_connection.cursor()
cur.execute(mysql_query_formatted, tuple(part_keys.keys()))

# Calculate revenue
revenue = 0
for row in cur.fetchall():
    _, l_partkey, l_extendedprice, l_discount = row
    # Only process rows that match the MongoDB part keys
    if l_partkey in part_keys:
        revenue += l_extendedprice * (1 - l_discount)

# Write output to CSV
with open('query_output.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['REVENUE'])
    writer.writerow([revenue])

# Close the connections
cur.close()
mysql_connection.close()
mongo_client.close()
