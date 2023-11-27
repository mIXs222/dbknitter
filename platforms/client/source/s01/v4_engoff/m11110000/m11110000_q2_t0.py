# query.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']

# Find region key for EUROPE from region collection in MongoDB
europe_region_key = None
for region in mongodb['region'].find({ "R_NAME": "EUROPE" }):
    europe_region_key = region['R_REGIONKEY']
    break

# Find nation keys for nations in EUROPE region from nation collection in MongoDB
europe_nation_keys = []
for nation in mongodb['nation'].find({ "N_REGIONKEY": europe_region_key }):
    europe_nation_keys.append(nation['N_NATIONKEY'])

# Get parts of brass type and size 15 from MongoDB
part_keys = []
for part in mongodb['part'].find({ "$and": [ { "P_TYPE": "BRASS" }, { "P_SIZE": 15 } ] }):
    part_keys.append(part['P_PARTKEY'])

# MySQL query to get the minimum cost suppliers
mysql_query = """
SELECT s.S_ACCTBAL, s.S_NAME, s.S_NATIONKEY, p.P_PARTKEY, p.P_MFGR,
       s.S_ADDRESS, s.S_PHONE, s.S_COMMENT
FROM partsupp ps
JOIN supplier s ON ps.PS_SUPPKEY = s.S_SUPPKEY AND s.S_NATIONKEY IN (%s)
JOIN part p ON ps.PS_PARTKEY = p.P_PARTKEY AND p.P_PARTKEY IN (%s)
WHERE ps.PS_SUPPLYCOST = (
    SELECT MIN(PS_SUPPLYCOST)
    FROM partsupp
    WHERE PS_PARTKEY = p.P_PARTKEY
)
ORDER BY s.S_ACCTBAL DESC, s.S_NATIONKEY, s.S_NAME, p.P_PARTKEY
"""

mysql_cursor.execute(mysql_query % (','.join(map(str, europe_nation_keys)), ','.join(map(str, part_keys))))

# Write query result to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(['S_ACCTBAL', 'S_NAME', 'NATION', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'])
    for row in mysql_cursor:
        s_acctbal, s_name, s_nationkey, p_partkey, p_mfgr, s_address, s_phone, s_comment = row
        # Get nation name from MongoDB
        nation_name = mongodb['nation'].find_one({ "N_NATIONKEY": s_nationkey })['N_NAME']
        csv_writer.writerow([s_acctbal, s_name, nation_name, p_partkey, p_mfgr, s_address, s_phone, s_comment])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
