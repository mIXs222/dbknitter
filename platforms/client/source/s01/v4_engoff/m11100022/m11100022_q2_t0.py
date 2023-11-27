# query.py

import pymysql
import pymongo
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Create a MySQL cursor
mysql_cursor = mysql_conn.cursor()

# Execute the MySQL query for the 'supplier' and 'partsupp' tables
mysql_cursor.execute("""
    SELECT
        S.S_SUPPKEY, S.S_NAME, S.S_ADDRESS, S.S_NATIONKEY, S.S_PHONE,
        S.S_ACCTBAL, S.S_COMMENT, PS.PS_PARTKEY, PS.PS_SUPPLYCOST
    FROM
        supplier S
    JOIN
        partsupp PS ON S.S_SUPPKEY = PS.PS_SUPPKEY
""")
suppliers_partsupps = mysql_cursor.fetchall()

# Connect to MongoDB database
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Fetch documents from 'part' collection with type BRASS and size 15
brass_parts = list(mongodb_db['part'].find({"P_TYPE": "BRASS", "P_SIZE": 15}, {"_id": 0}))

# Fetch documents from 'nation' collection and transform it to dictionary for quick access
nations = {doc["N_NATIONKEY"]: doc for doc in mongodb_db['nation'].find({}, {"_id": 0})}

# Fetch documents from 'region' collection and find the EUROPE region key
europe_key = mongodb_db['region'].find_one({"R_NAME": "EUROPE"}, {"_id": 0})['R_REGIONKEY']
european_nations = [key for key, val in nations.items() if val['N_REGIONKEY'] == europe_key]

# Combine and filter the results
combined_results = []
for part in brass_parts:
    for supplier, _, _, nation_key, _, acctbal, address, phone, comment, partkey, supplycost in suppliers_partsupps:
        if nation_key in european_nations and partkey == part['P_PARTKEY']:
            combined_results.append((acctbal, nations[nation_key]['N_NAME'], supplier, partkey, part['P_MFGR'], address, phone, comment))

# Sort the results according to the given sorting order
combined_results.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))

# Write output to query_output.csv file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'])
    for line in combined_results:
        writer.writerow(line)

# Close the MySQL connection
mysql_cursor.close()
mysql_conn.close()
