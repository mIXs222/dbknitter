import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Get parts data from MongoDB
parts_query = {
    'P_BRAND': {'$ne': 'Brand#45'},
    'P_TYPE': {'$not': {'$regex': '^MEDIUM POLISHED'}},
    'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}
}
part_projection = {'_id': 0}
parts = mongodb['part'].find(parts_query, part_projection)

# Convert MongoDB parts to a list of dicts, for use in the MySQL query
valid_parts = list(parts)

# Prepare MySQL query
supplier_query = """
SELECT S_SUPPKEY 
FROM supplier
WHERE S_COMMENT NOT LIKE '%Customer Complaints%'
"""

# Execute MySQL query for valid suppliers
with mysql_conn.cursor() as cursor:
    cursor.execute(supplier_query)
    valid_suppliers = cursor.fetchall()

# Convert valid suppliers to a s
et for faster lookups
valid_supplier_keys = {s['S_SUPPKEY'] for s in valid_suppliers}

# Prepare MongoDB query for partsupp with valid suppliers only
partsupp_query = {'PS_SUPPKEY': {'$in': list(valid_supplier_keys)}}
partsupp_projection = {'_id': 0}
partsupps = mongodb['partsupp'].find(partsupp_query, partsupp_projection)

# Join parts and partsupp based on the part key, count valid suppliers
results = []
for part in valid_parts:
    for partsupp in partsupps:
        if part['P_PARTKEY'] == partsupp['PS_PARTKEY']:
            results.append({'P_BRAND': part['P_BRAND'],
                            'P_TYPE': part['P_TYPE'],
                            'P_SIZE': part['P_SIZE'],
                            'SUPPLIER_CNT': partsupp['PS_SUPPKEY']})

# Sort results
results.sort(key=lambda x: (-x['SUPPLIER_CNT'], x['P_BRAND'], x['P_TYPE'], x['P_SIZE']))

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

# Close connections
mysql_conn.close()
mongo_client.close()
