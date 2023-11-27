import pymysql
import pymongo
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Fetch data from partsupp table in MySQL
partsupp_query = """
SELECT PS_PARTKEY, PS_SUPPKEY
FROM partsupp
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(partsupp_query)
    partsupp_data = cursor.fetchall()

# Creating a set of suppliers with complaints (assumption based on request)
complained_suppliers = set()
for line in partsupp_data:
    if "complaint" in line[4].lower():
        complained_suppliers.add(line[1])

# Connect to MongoDB database
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']
part_collection = mongodb_db['part']
supplier_collection = mongodb_db['supplier']

# Fetch data from part and supplier collections in MongoDB
part_data = list(part_collection.find({
    'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]},
    'P_TYPE': {'$ne': 'MEDIUM POLISHED'},
    'P_BRAND': {'$ne': 'Brand#45'}
}))

supplied_parts_by_suppliers = {}
for part in part_data:
    partkey = part['P_PARTKEY']
    for supp_info in partsupp_data:
        if supp_info[0] == partkey and supp_info[1] not in complained_suppliers:
            supplied_parts_by_suppliers.setdefault(partkey, set()).add(supp_info[1])

# Count the suppliers for each part category and write to CSV file
output_data = []
for part in part_data:
    if part['P_PARTKEY'] in supplied_parts_by_suppliers:
        output_data.append((
            part['P_BRAND'],
            part['P_TYPE'],
            part['P_SIZE'],
            len(supplied_parts_by_suppliers[part['P_PARTKEY']])
        ))

output_data.sort(key=lambda x: (-x[3], x[0], x[1], x[2]))

# Write the query output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_COUNT'])
    for row in output_data:
        writer.writerow(row)

mysql_conn.close()
mongodb_client.close()
