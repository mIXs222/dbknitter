import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Find the EUROPE region key
mysql_cursor.execute("SELECT R_REGIONKEY FROM region WHERE R_NAME = 'EUROPE'")
europe_region_key = None
for (key,) in mysql_cursor.fetchall():
    europe_region_key = key
    break

if europe_region_key is None:
    print("EUROPE region not found.")
    exit()

# Get part keys for BRASS parts of size 15
mongo_parts = mongo_db.part.find({
    'P_TYPE': 'BRASS',
    'P_SIZE': 15
}, {'P_PARTKEY': 1, 'P_NAME': 1, 'P_MFGR': 1})

part_keys = list(mongo_parts)

# Query for supplier information for relevant parts
results = []

for part in part_keys:
    p_partkey = part['P_PARTKEY']

    # Find minimum cost supplier and related information
    mysql_cursor.execute(f"""
        SELECT s.S_NAME, s.S_ACCTBAL, n.N_NAME, s.S_ADDRESS, s.S_PHONE, s.S_COMMENT, ps.PS_SUPPLYCOST, ps.PS_PARTKEY 
        FROM partsupp ps 
        JOIN supplier s ON ps.PS_SUPPKEY = s.S_SUPPKEY 
        JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY 
        WHERE ps.PS_PARTKEY = {p_partkey} AND n.N_REGIONKEY = {europe_region_key} 
        ORDER BY ps.PS_SUPPLYCOST ASC, s.S_ACCTBAL DESC, n.N_NAME ASC, s.S_NAME ASC, ps.PS_PARTKEY ASC
        LIMIT 1;
    """)

    for row in mysql_cursor.fetchall():
        s_name, s_acctbal, n_name, s_address, s_phone, s_comment, ps_supplycost, ps_partkey = row
        parts_info = {
            's_acctbal': s_acctbal,
            's_name': s_name,
            'n_name': n_name,
            'p_partkey': ps_partkey,
            'p_mfgr': part['P_MFGR'],
            's_address': s_address,
            's_phone': s_phone,
            's_comment': s_comment
        }
        results.append(parts_info)

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow(result)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
