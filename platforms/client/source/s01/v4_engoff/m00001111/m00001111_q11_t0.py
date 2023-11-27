import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
partsupp_collection = mongo_db['partsupp']

# Fetch suppliers in Germany from MySQL
mysql_cursor.execute("""
SELECT S_SUPPKEY
FROM supplier
JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
WHERE nation.N_NAME = 'GERMANY'
""")
suppliers_in_germany = mysql_cursor.fetchall()
germany_supplier_keys = [str(supplier_tuple[0]) for supplier_tuple in suppliers_in_germany]

# Fetch PartSupp documents for the suppliers in Germany from MongoDB
partsupp_docs = partsupp_collection.find({'PS_SUPPKEY': {'$in': germany_supplier_keys}})

# Calculate total value for all available parts
total_value = sum(doc['PS_AVAILQTY'] * doc['PS_SUPPLYCOST'] for doc in partsupp_docs)

# Filtering and sorting part numbers with significant value
significant_parts = []
for doc in partsupp_docs:
    part_value = doc['PS_AVAILQTY'] * doc['PS_SUPPLYCOST']
    if part_value / total_value > 0.0001:
        significant_parts.append((doc['PS_PARTKEY'], part_value))

# Sorting significant parts in descending order of value
significant_parts_sorted = sorted(significant_parts, key=lambda x: -x[1])

# Write the results to 'query_output.csv'
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PS_PARTKEY', 'VALUE'])
    for part in significant_parts_sorted:
        writer.writerow(part)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
