# query.py
import csv
import pymysql
import pymongo

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query MySQL for suppliers in the 'EUROPE' region
mysql_cursor = mysql_connection.cursor()
mysql_query = """
SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_ACCTBAL, s.S_COMMENT
FROM supplier s
INNER JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
INNER JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
WHERE r.R_NAME = 'EUROPE'
"""
mysql_cursor.execute(mysql_query)
suppliers = {row[0]: row[1:] for row in mysql_cursor.fetchall()}

# Query MongoDB for parts with a size of 15 and type containing 'BRASS'
parts_cursor = mongo_db.part.find({'P_SIZE': 15, 'P_TYPE': {'$regex': 'BRASS'}}, {'_id': 0})
parts = list(parts_cursor)

# Query MongoDB for partsupp with the minimum PS_SUPPLYCOST within 'EUROPE' region
parts_suppliers = mongo_db.partsupp.aggregate([
    {'$lookup': {
        'from': "supplier",
        'localField': "PS_SUPPKEY",
        'foreignField': "S_SUPPKEY",
        'as': "supplier_info"
    }},
    {'$match': {'supplier_info': {'$ne': []}}},
    {'$unwind': '$supplier_info'},
    {'$sort': {'PS_SUPPLYCOST': 1}},
    {'$group': {
        '_id': '$PS_PARTKEY',
        'PS_SUPPKEY': {'$first': '$PS_SUPPKEY'},
        'PS_SUPPLYCOST': {'$first': '$PS_SUPPLYCOST'}
    }}
])

# Combine the data
combined_results = [
    (
        supp[4], supp[0], supp[1], supp[2], supp[3], supp[5], 
        part['P_PARTKEY'], part['P_NAME'], part['P_MFGR'], part['P_SIZE']
    )
    for part in parts
    for ps in parts_suppliers
    if part['P_PARTKEY'] == ps['_id'] and ps['PS_SUPPKEY'] in suppliers
    for supp in [suppliers[ps['PS_SUPPKEY']]]
]

# Sort the combined results
combined_results.sort(key=lambda x: (-x[0], x[1], x[2], x[6]))

# Write output to CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([
        'S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 
        'P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_SIZE'
    ])
    for result in combined_results:
        writer.writerow(result)

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
