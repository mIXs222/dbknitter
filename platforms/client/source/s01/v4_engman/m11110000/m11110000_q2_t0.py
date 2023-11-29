import pymysql
import pymongo
import csv

# MySQL connection
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_connection.cursor()

# MongoDB connection
mongodb_connection = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_connection['tpch']

# Find regionkey for EUROPE
region_key = None
for region in mongodb_db.region.find({"R_NAME": "EUROPE"}):
    region_key = region["R_REGIONKEY"]
    break

# Find nationkeys for nations in EUROPE
nation_keys = []
for nation in mongodb_db.nation.find({"N_REGIONKEY": region_key}):
    nation_keys.append(nation["N_NATIONKEY"])

# Query MongoDB for suppliers from the nations in EUROPE region and store them in a dictionary
suppliers_info = {}
for supplier in mongodb_db.supplier.find({"S_NATIONKEY": {"$in": nation_keys}}):
    suppliers_info[supplier["S_SUPPKEY"]] = supplier

# Query MongoDB for parts that are of BRASS type and size 15
parts_info = {}
for part in mongodb_db.part.find({"P_TYPE": "BRASS", "P_SIZE": 15}):
    parts_info[part["P_PARTKEY"]] = part

# Preparing the SQL Query with known part keys and supplier keys
part_keys_str = ', '.join(str(k) for k in parts_info.keys())
supplier_keys_str = ', '.join(str(k) for k in suppliers_info.keys())

sql_query = f"""
SELECT PS_PARTKEY, PS_SUPPKEY, MIN(PS_SUPPLYCOST) AS MIN_COST
FROM partsupp
WHERE PS_PARTKEY IN ({part_keys_str}) AND PS_SUPPKEY IN ({supplier_keys_str})
GROUP BY PS_PARTKEY
"""

mysql_cursor.execute(sql_query)

results = []

for row in mysql_cursor.fetchall():
    part_key, supp_key, _ = row
    part = parts_info[part_key]
    supplier = suppliers_info[supp_key]
    nation = mongodb_db.nation.find_one({"N_NATIONKEY": supplier["S_NATIONKEY"]})
    results.append({
        'N_NAME': nation['N_NAME'],
        'P_MFGR': part['P_MFGR'],
        'P_PARTKEY': part['P_PARTKEY'],
        'S_ACCTBAL': supplier['S_ACCTBAL'],
        'S_ADDRESS': supplier['S_ADDRESS'],
        'S_COMMENT': supplier['S_COMMENT'],
        'S_NAME': supplier['S_NAME'],
        'S_PHONE': supplier['S_PHONE'],
    })

# Sort results
results.sort(key=lambda x: (-x['S_ACCTBAL'], x['N_NAME'], x['S_NAME'], x['P_PARTKEY']))

# Write the results to a CSV file
output_fields = ['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']
with open('query_output.csv', mode='w') as f:
    writer = csv.DictWriter(f, fieldnames=output_fields)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongodb_connection.close()
