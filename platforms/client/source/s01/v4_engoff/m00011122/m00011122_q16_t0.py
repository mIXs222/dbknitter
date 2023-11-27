# Import required modules
import pymysql
import pymongo
import csv

# Connection information
mysql_conn_info = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

mongo_conn_info = {
    'database': 'tpch',
    'hostname': 'mongodb',
    'port': 27017,
}

# Query Part Information from MySQL
try:
    # Connect to the MySQL Database
    mysql_connection = pymysql.connect(
        host=mysql_conn_info['host'],
        user=mysql_conn_info['user'],
        password=mysql_conn_info['password'],
        database=mysql_conn_info['database']
    )

    with mysql_connection.cursor() as cursor:
        part_query = """
            SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE
            FROM part
            WHERE P_BRAND <> 'Brand#45'
            AND P_TYPE NOT LIKE 'MEDIUM POLISHED%%'
            AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
        """
        cursor.execute(part_query)
        part_data = cursor.fetchall()
finally:
    mysql_connection.close()

# Query Supplier Information from MongoDB
try:
    # Connect to the MongoDB Database
    mongo_client = pymongo.MongoClient(
        host=mongo_conn_info['hostname'],
        port=mongo_conn_info['port']
    )
    mongodb = mongo_client[mongo_conn_info['database']]

    supplier_data = list(mongodb.supplier.find(
        {'S_COMMENT': {'$not': {'$regex': '.*Customer.*Complaints.*'}}},
        {'S_SUPPKEY': 1}
    ))

    partsupp_data = list(mongodb.partsupp.find(
        {'PS_SUPPKEY': {'$in': [x['S_SUPPKEY'] for x in supplier_data]}},
        {'PS_SUPPKEY': 1, 'PS_PARTKEY': 1}
    ))
finally:
    mongo_client.close()

# Combine data
valid_supp_keys = set([d['PS_SUPPKEY'] for d in partsupp_data])
valid_part_keys = set([d[0] for d in part_data])
combined_data = [d for d in partsupp_data if d['PS_PARTKEY'] in valid_part_keys]

# Count how many suppliers supply each part
part_supp_count = {}
for entry in combined_data:
    part_key = entry['PS_PARTKEY']
    if part_key not in part_supp_count:
        part_supp_count[part_key] = {'count': 1, 'part': next((p[1:4] for p in part_data if p[0] == part_key), None)}
    else:
        part_supp_count[part_key]['count'] += 1

# Prepare data for output
output_rows = [
    (part['part'][0], part['part'][1], part['part'][2], part['count'])
    for part_key, part in sorted(part_supp_count.items(), key=lambda x: (-x[1]['count'], x[1]['part']))
]

# Write output to CSV
with open('query_output.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_COUNT'])
    for row in output_rows:
        writer.writerow(row)
