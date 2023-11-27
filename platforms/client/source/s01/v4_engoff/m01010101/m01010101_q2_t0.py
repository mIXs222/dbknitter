import pymysql
import pymongo
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch parts and partsupp data from MySQL
mysql_cursor.execute("""
    SELECT p.P_PARTKEY, p.P_MFGR, ps.PS_SUPPKEY, ps.PS_SUPPLYCOST
    FROM part p
    JOIN partsupp ps ON p.P_PARTKEY = ps.PS_PARTKEY
    WHERE p.P_TYPE = 'BRASS' AND p.P_SIZE = 15
""")
parts_partsupp = mysql_cursor.fetchall()

# Connect to MongoDB database
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client.tpch

# Fetch region and supplier data from MongoDB
europe_regionkey = mongo_db.region.find_one({'R_NAME': 'EUROPE'}, {'R_REGIONKEY': 1})
suppliers = list(mongo_db.supplier.find({'S_NATIONKEY': europe_regionkey['R_REGIONKEY']}))

min_cost_suppliers = {}
# Find the minimum cost for each part from suppliers
for part in parts_partsupp:
    part_key = part[0]
    mfgr = part[1]
    supp_key = part[2]
    supply_cost = part[3]

    # Retrieve relevant supplier details from MongoDB
    supplier_details = next((s for s in suppliers if s['S_SUPPKEY'] == supp_key), None)
    if supplier_details is None:
        continue
    
    if part_key not in min_cost_suppliers or supply_cost < min_cost_suppliers[part_key][2]:
        min_cost_suppliers[part_key] = (
            supplier_details['S_SUPPKEY'],
            supplier_details['S_NAME'],
            supply_cost,
            mfgr,
            supplier_details['S_ACCTBAL'],
            supplier_details['S_NATIONKEY'],
            supplier_details['S_ADDRESS'],
            supplier_details['S_PHONE'],
            supplier_details['S_COMMENT']
        )

# Fetch nation names from MySQL using nation keys from MongoDB
nation_names = {}
for sup_info in min_cost_suppliers.values():
    nation_key = sup_info[5]
    if nation_key not in nation_names:
        mysql_cursor.execute(f"SELECT N_NAME FROM nation WHERE N_NATIONKEY = {nation_key}")
        nation_name = mysql_cursor.fetchone()[0]
        nation_names[nation_key] = nation_name

# Prepare final results for CSV
results = []
for part_key, sup_info in min_cost_suppliers.items():
    nation_name = nation_names[sup_info[5]]
    results.append([
        sup_info[4],
        sup_info[1],
        nation_name,
        part_key,
        sup_info[3],
        sup_info[6],
        sup_info[7],
        sup_info[8]
    ])

# Sort results according to query specifics
results.sort(key=lambda x: (-x[0], x[2], x[1], x[3]))

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    for row in results:
        csv_writer.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
