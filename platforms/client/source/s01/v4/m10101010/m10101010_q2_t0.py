# query.py
import pymysql
import pymongo
import csv


# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']

# Step 1: Perform subquery to get the minimum supply cost from partsupp in Europe
sql_subquery = """
SELECT MIN(PS_SUPPLYCOST) FROM partsupp, supplier, nation, region
WHERE S_SUPPKEY = PS_SUPPKEY AND S_NATIONKEY = N_NATIONKEY 
AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'EUROPE'
"""
mysql_cursor.execute(sql_subquery)
min_supply_cost = mysql_cursor.fetchone()[0]

# Step 2: Query MySQL and MongoDB separately and combine the results
sql_query = """
SELECT
    S_ACCTBAL, S_NAME, S_ADDRESS, S_PHONE, S_COMMENT,
    N_NAME,
    P_PARTKEY, P_MFGR
FROM
    supplier, nation, region
WHERE
    S_NATIONKEY = N_NATIONKEY
    AND N_REGIONKEY = R_REGIONKEY
    AND R_NAME = 'EUROPE'
"""
mysql_cursor.execute(sql_query)
mysql_results = mysql_cursor.fetchall()

# Get nation data from MongoDB
nation_data = mongo_db.nation.find({"N_REGIONKEY": {"$in": [doc[0] for doc in mysql_cursor.fetchall()]}})

# Map nation keys to names for MongoDB
nation_key_to_name = {doc["N_NATIONKEY"]: doc["N_NAME"] for doc in nation_data}

# Get part and partsupp data from MongoDB
part_data = mongo_db.part.find(
    {"P_SIZE": 15, "P_TYPE": {'$regex': 'BRASS$'}, "P_PARTKEY": {"$exists": True}})
partsupp_data = list(mongo_db.partsupp.find(
    {"PS_SUPPLYCOST": min_supply_cost, "PS_PARTKEY": {"$exists": True}, "PS_SUPPKEY": {"$exists": True}}))

# Create a map for parts and partsupp
part_data_map = {doc['P_PARTKEY']: doc for doc in part_data}
partsupp_data_map = {doc['PS_PARTKEY']: doc for doc in partsupp_data}

# Combine data from both databases
results = []
for (s_acctbal, s_name, s_address, s_phone, s_comment, n_name, p_partkey, p_mfgr) in mysql_results:
    n_name = nation_key_to_name.get(n_name)
    p_data = part_data_map.get(p_partkey)
    ps_data = partsupp_data_map.get(p_partkey)
    if p_data and ps_data:
        results.append([
            s_acctbal, s_name, n_name, p_partkey, p_mfgr,
            s_address, s_phone, s_comment
        ])

# Sort the results
results.sort(key=lambda x: (-x[0], x[2], x[1], x[3]))

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Write header
    csvwriter.writerow(['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'])
    # Write data rows
    for row in results:
        csvwriter.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
