import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Execute MySQL query to retrieve parts and nations within the 'EUROPE' region
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("""
    SELECT 
        p.P_PARTKEY, p.P_MFGR, p.P_SIZE, n.N_NATIONKEY, n.N_NAME
    FROM 
        part p
    JOIN nation n ON p.P_SIZE = 15 AND p.P_TYPE LIKE '%BRASS%' 
    JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY AND r.R_NAME = 'EUROPE'
    ORDER BY 
        n.N_NAME ASC
""")
part_nation_data = mysql_cursor.fetchall()
mysql_cursor.close()

# Creating a dictionary with part keys and nation keys for MongoDB queries
part_nation_dict = {(pn[0], pn[3]): (pn[1], pn[2], pn[4]) for pn in part_nation_data}

# Executing MongoDB query for suppliers and partsupp collection
supplier_data = mongo_db.supplier.find({"S_NATIONKEY": {"$in": [pn[3] for pn in part_nation_dict.keys()]}})
supplier_dict = {s["S_SUPPKEY"]: s for s in supplier_data}

partsupp_data = mongo_db.partsupp.find({"PS_PARTKEY": {"$in": [pn[0] for pn in part_nation_dict.keys()]}})
partsupp_dict = {(ps["PS_PARTKEY"], ps["PS_SUPPKEY"]): ps for ps in partsupp_data}

# Merging the data
final_data = []
for (part_key, supp_key), partsupp in partsupp_dict.items():
    if supp_key in supplier_dict:
        supplier = supplier_dict[supp_key]
        part_mfgr, part_size, nation_name = part_nation_dict.get((part_key, supplier['S_NATIONKEY']), (None, None, None))

        if part_mfgr and part_size:  # Ensure part_mfgr and part_size exist
            final_data.append([
                supplier['S_ACCTBAL'], supplier['S_NAME'], supplier['S_ADDRESS'],
                supplier['S_PHONE'], supplier['S_COMMENT'], part_key,
                part_mfgr, part_size, nation_name
            ])

# Sort the final data
final_data.sort(key=lambda x: (-x[0], x[7], x[1], x[5]))

# Writing results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'N_NAME'])
    for row in final_data:
        csv_writer.writerow(row)

# Close the connections
mysql_conn.close()
mongo_client.close()
