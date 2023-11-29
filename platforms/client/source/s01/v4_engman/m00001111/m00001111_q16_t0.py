import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql', user='root', password='my-secret-pw', database='tpch')

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Fetch parts that satisfy conditions from MySQL
with mysql_conn.cursor() as cursor:
    part_query = """
        SELECT P_PARTKEY FROM part
        WHERE P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
        AND P_TYPE NOT LIKE '%MEDIUM POLISHED%'
        AND P_BRAND <> 'Brand#45'
    """
    cursor.execute(part_query)
    valid_parts = [row[0] for row in cursor.fetchall()]

# Fetch suppliers without complaints and their corresponding parts from MongoDB
partsupp_collection = mongodb_db['partsupp']
valid_partsupp = list(partsupp_collection.find({
    'PS_PARTKEY': {'$in': valid_parts},
    'PS_COMMENT': {'$not': {'$regex': '.*Customer.*Complaints.*'}}
}, {'PS_PARTKEY': 1, 'PS_SUPPKEY': 1, '_id': 0}))

# Convert list of dictionaries to a set of tuples for faster search
valid_partsupp_set = {(ps['PS_PARTKEY'], ps['PS_SUPPKEY']) for ps in valid_partsupp}

# Fetch suppliers from MySQL and filter out those with complaints
with mysql_conn.cursor() as cursor:
    supplier_query = "SELECT S_SUPPKEY FROM supplier WHERE S_COMMENT NOT LIKE '%Customer%Complaints%'"
    cursor.execute(supplier_query)
    valid_suppliers = [row[0] for row in cursor.fetchall()]

# Calculate the number of suppliers meeting the conditions
parts_supplied_by_valid_suppliers = [
    (part_key, sum(1 for _, supp_key in valid_partsupp_set if supp_key in valid_suppliers))
    for part_key in valid_parts
]

# Sort the results
sorted_parts_supplied = sorted(
    parts_supplied_by_valid_suppliers, key=lambda x: (-x[1], x[0]))

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['P_PARTKEY', 'SUPPLIER_COUNT'])  # Header
    for part_key, count in sorted_parts_supplied:
        writer.writerow([part_key, count])

# Close connections
mysql_conn.close()
mongodb_client.close()
