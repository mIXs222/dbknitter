# File: query_executer.py
import csv
import pymysql
from pymongo import MongoClient

# MySQL connection parameters
mysql_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# MongoDB connection parameters
mongodb_params = {
    'host': 'mongodb',
    'port': 27017
}

# Connect to MySQL
conn = pymysql.connect(**mysql_params)
cur = conn.cursor()

# Find the minimum supply cost for parts from EUROPE region in MySQL
query_min_supply_cost = """
SELECT MIN(PS_SUPPLYCOST) 
FROM partsupp, supplier, nation 
WHERE S_SUPPKEY = PS_SUPPKEY 
AND S_NATIONKEY = N_NATIONKEY 
AND N_REGIONKEY IN (
    SELECT R_REGIONKEY 
    FROM region WHERE R_NAME = 'EUROPE'
)
"""
cur.execute(query_min_supply_cost)
min_supply_cost = cur.fetchone()[0]

# Fetch supplier data meeting supply cost and other conditions in MySQL
query_suppliers = """
SELECT S_SUPPKEY, S_ACCTBAL, S_NAME, S_ADDRESS, S_PHONE, S_COMMENT, N_NATIONKEY 
FROM supplier
JOIN nation ON S_NATIONKEY = N_NATIONKEY 
JOIN region ON N_REGIONKEY = R_REGIONKEY 
WHERE R_NAME = 'EUROPE'
"""
cur.execute(query_suppliers)
suppliers = {row[0]: row[1:] for row in cur.fetchall()}

# Connect to MongoDB
mongo_client = MongoClient(**mongodb_params)
mongo_db = mongo_client['tpch']

# Find the parts that match the conditions in MongoDB
parts_query = {
    'P_SIZE': 15,
    'P_TYPE': {'$regex': 'BRASS$'},
}
parts_projection = {
    'P_PARTKEY': 1, 'P_MFGR': 1
}
part_data = list(mongo_db.part.find(parts_query, parts_projection))

# Find matching partsupp entries with minimum supply cost in MySQL
partsupp_data = {}
for part in part_data:
    query_partsupp = f"""
    SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST 
    FROM partsupp
    WHERE PS_PARTKEY = {part['P_PARTKEY']} 
    AND PS_SUPPLYCOST = {min_supply_cost}
    """
    cur.execute(query_partsupp)
    part['suppliers'] = cur.fetchall()
    for supplier in part['suppliers']:
        partsupp_data[supplier] = part['P_PARTKEY']

# Close MySQL connection
cur.close()
conn.close()

# Combine the data and write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    
    # Write header
    csvwriter.writerow(['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'])
    
    # Write rows
    for suppkey, partsupp in partsupp_data.items():
        supplier = suppliers.get(suppkey)
        part_key = partsupp
        part_mfgr = next((p['P_MFGR'] for p in part_data if p['P_PARTKEY'] == part_key), None)
        
        if supplier:
            row = [
                supplier[0],  # S_ACCTBAL
                supplier[1],  # S_NAME
                supplier[4],  # N_NAME
                part_key,     # P_PARTKEY
                part_mfgr,    # P_MFGR
                supplier[2],  # S_ADDRESS
                supplier[3],  # S_PHONE
                supplier[5],  # S_COMMENT
            ]
            csvwriter.writerow(row)
