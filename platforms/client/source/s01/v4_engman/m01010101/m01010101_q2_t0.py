import pymysql
import pymongo
import csv

# Connect to MySQL database
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
cursor = conn.cursor()

# Fetch data from MySQL tables part and partsupp where part size is 15 and type is BRASS
cursor.execute("""
    SELECT P_PARTKEY, P_MFGR 
    FROM part 
    WHERE P_TYPE = 'BRASS' AND P_SIZE = 15;
    """)
parts_data = cursor.fetchall()

# Fetch nations from Europe region
cursor.execute("""
    SELECT N_NATIONKEY, N_NAME 
    FROM nation 
    WHERE N_REGIONKEY = (SELECT R_REGIONKEY FROM region WHERE R_NAME = 'EUROPE');
    """)
nation_data = cursor.fetchall()

# Close MySQL connection
cursor.close()
conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
db = mongo_client['tpch']

# Fetch required supplier information from MongoDB
supplier_data = list(db.supplier.find(
    {'S_NATIONKEY': {'$in': [n[0] for n in nation_data]}},
    {'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1, 'S_PHONE': 1, 'S_ACCTBAL': 1, 'S_COMMENT': 1, 'S_NATIONKEY': 1}
))

# Mapping nation keys with names
nation_mapping = {n[0]: n[1] for n in nation_data}

parts_suppliers = {}

# Find the minimum cost supplier for each part from MySQL part and partsupp data
for part_key, part_mfgr in parts_data:
    cursor.execute("""
        SELECT PS_SUPPKEY, PS_SUPPLYCOST 
        FROM partsupp 
        WHERE PS_PARTKEY = %s;
        """, (part_key,))
    part_suppliers = cursor.fetchall()
    min_cost = min(part_suppliers, key=lambda x: x[1])[1]
    parts_suppliers[part_key] = {'P_MFGR': part_mfgr, 'suppliers': [(s[0], min_cost) for s in part_suppliers if s[1] == min_cost]}

# Writing results to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    for part_key, part_info in parts_suppliers.items():
        for supp_info in part_info['suppliers']:
            supp_key, min_cost = supp_info
            # Find the supplier from MongoDB supplier data
            for s_data in supplier_data:
                if s_data['S_SUPPKEY'] == supp_key:
                    row = [
                        nation_mapping[s_data['S_NATIONKEY']],
                        part_info['P_MFGR'],
                        part_key,
                        s_data['S_ACCTBAL'],
                        s_data['S_ADDRESS'],
                        s_data['S_COMMENT'],
                        s_data['S_NAME'],
                        s_data['S_PHONE']
                    ]
                    writer.writerow(row)
                    break

mongo_client.close()
