import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Fetch the EUROPE region key
regions_collection = mongodb['region']
europe_region = regions_collection.find_one({'R_NAME': 'EUROPE'})
europe_region_key = europe_region['R_REGIONKEY']

# Fetch the nation keys for the EUROPE region
nations_collection = mongodb['nation']
europe_nations = nations_collection.find({'N_REGIONKEY': europe_region_key})
europe_nation_keys = list(map(lambda x: x['N_NATIONKEY'], europe_nations))

# Fetch the parts with size 15 and type containing 'BRASS'
parts_collection = mongodb['part']
parts = parts_collection.find({'P_SIZE': 15, 'P_TYPE': {'$regex': 'BRASS'}})
part_keys = list(map(lambda x: x['P_PARTKEY'], parts))

# Fetch suppliers from EUROPE nations
suppliers_collection = mongodb['supplier']
suppliers = suppliers_collection.find({'S_NATIONKEY': {'$in': europe_nation_keys}})
supplier_keys = list(map(lambda x: x['S_SUPPKEY'], suppliers))

# Execute the MySQL query to get part suppliers information
mysql_cursor = mysql_conn.cursor()
query = """
SELECT ps.PS_PARTKEY, ps.PS_SUPPKEY, ps.PS_SUPPLYCOST, s.S_ACCTBAL, s.S_NAME,
       s.S_ADDRESS, s.S_PHONE, s.S_COMMENT, p.P_MFGR
FROM partsupp ps
JOIN supplier s ON ps.PS_SUPPKEY = s.S_SUPPKEY
JOIN part p ON ps.PS_PARTKEY = p.P_PARTKEY
WHERE ps.PS_SUPPKEY IN (%s)
AND ps.PS_PARTKEY IN (%s)
ORDER BY s.S_ACCTBAL DESC, s.S_NAME ASC, ps.PS_PARTKEY ASC
"""
format_strings = ','.join(['%s'] * len(supplier_keys))
part_format_strings = ','.join(['%s'] * len(part_keys))
mysql_cursor.execute(query % (format_strings, part_format_strings), tuple(supplier_keys) + tuple(part_keys))

# Fetch the results
results = mysql_cursor.fetchall()

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['PS_PARTKEY', 'PS_SUPPKEY', 'PS_SUPPLYCOST', 'S_ACCTBAL', 'S_NAME',
                         'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_MFGR'])
    for row in results:
        csv_writer.writerow(row)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
