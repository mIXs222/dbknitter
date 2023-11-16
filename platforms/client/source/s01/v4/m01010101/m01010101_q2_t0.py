import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
region_collection = mongodb_db['region']
supplier_collection = mongodb_db['supplier']

# Fetch region data from MongoDB
regions = {r['R_REGIONKEY']: r['R_NAME'] for r in region_collection.find({'R_NAME': 'EUROPE'})}

# Fetch supplier data from MongoDB
suppliers = {}
for s in supplier_collection.find():
    if s['S_NATIONKEY'] in regions:
        suppliers[s['S_SUPPKEY']] = s

# Execute the main query on MySQL
mysql_query = """
SELECT 
    PS_SUPPKEY, 
    N_NATIONKEY, 
    PS_PARTKEY, 
    P_MFGR,
    PS_SUPPLYCOST
FROM 
    partsupp, 
    part,
    nation
WHERE 
    P_PARTKEY = PS_PARTKEY 
    AND P_SIZE = 15 
    AND P_TYPE LIKE '%BRASS'
    AND PS_SUPPKEY IN ('{}')
    AND S_NATIONKEY = N_NATIONKEY
""".format("','".join(map(str, suppliers.keys())))

mysql_cursor.execute(mysql_query)
partsupp_results = mysql_cursor.fetchall()

# Find min PS_SUPPLYCOST for supplied S_SUPPKEY
min_supply_cost = {}
for row in partsupp_results:
    suppkey, nationkey, partkey, mfgr, supplycost = row
    if suppkey not in min_supply_cost or supplycost < min_supply_cost[suppkey]:
        min_supply_cost[suppkey] = supplycost

# Filter results on min PS_SUPPLYCOST
filtered_results = [
    (suppliers[row[0]], nationkey, partkey, mfgr) 
    for row in partsupp_results if row[4] == min_supply_cost[row[0]]
]

# Writing results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'])
    for supp_info, nationkey, partkey, mfgr in filtered_results:
        s_acctbal = supp_info['S_ACCTBAL']
        s_name = supp_info['S_NAME']
        n_name = [row[1] for row in mysql_cursor if row[0] == nationkey]
        s_address = supp_info['S_ADDRESS']
        s_phone = supp_info['S_PHONE']
        s_comment = supp_info['S_COMMENT']
        csvwriter.writerow([s_acctbal, s_name, n_name, partkey, mfgr, s_address, s_phone, s_comment])

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
