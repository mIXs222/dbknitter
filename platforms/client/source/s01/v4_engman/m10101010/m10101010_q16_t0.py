# Python code to execute the query (query.py)

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
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Query to get the necessary data from MySQL
# Only fetch suppliers that do not have complaints (S_COMMENT not containing 'Customer' and 'Complaints')
mysql_query = """
SELECT S_SUPPKEY 
FROM supplier 
WHERE S_COMMENT NOT LIKE '%%Customer%%Complaints%%';
"""

# Execute MySQL Query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    suppliers = cursor.fetchall()

# Filtering the list of suppliers
supplier_ids = [str(s[0]) for s in suppliers]

# Query in MongoDB to get data from the 'part' and 'partsupp' collections
# We also filter the parts based on the given attributes and sizes.
acceptable_sizes = [49, 14, 23, 45, 19, 3, 36, 9]

# Query parts that satisfy the conditions
mongodb_query_parts = {
    'P_SIZE': {'$in': acceptable_sizes},
    'P_TYPE': {'$ne': 'MEDIUM POLISHED'},
    'P_BRAND': {'$ne': 'Brand#45'}
}

parts_cursor = mongodb_db['part'].find(mongodb_query_parts, {'P_PARTKEY': 1})
part_keys = [p['P_PARTKEY'] for p in parts_cursor]

# Query partsupp collection to find the parts supplied by the filtered suppliers
mongodb_query_partsupp = {
    'PS_PARTKEY': {'$in': part_keys},
    'PS_SUPPKEY': {'$in': supplier_ids}
}

# Count and group by brand, type, and size
pipeline = [
    {'$match': mongodb_query_partsupp},
    {'$lookup': {
        'from': 'part',
        'localField': 'PS_PARTKEY',
        'foreignField': 'P_PARTKEY',
        'as': 'part'
    }},
    {'$unwind': '$part'},
    {'$group': {
        '_id': {
            'brand': '$part.P_BRAND',
            'type': '$part.P_TYPE',
            'size': '$part.P_SIZE'
        },
        'count': {'$sum': 1}
    }},
    {'$sort': {'count': -1, '_id.brand': 1, '_id.type': 1, '_id.size': 1}}
]

result = list(mongodb_db['partsupp'].aggregate(pipeline))

# Write the output to 'query_output.csv'
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['brand', 'type', 'size', 'supplier_count'])

    for doc in result:
        writer.writerow([doc['_id']['brand'], doc['_id']['type'], doc['_id']['size'], doc['count']])

# Close the database connections
mysql_conn.close()
mongodb_client.close()
