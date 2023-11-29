import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch necessary data from MySQL
mysql_cursor.execute("""
    SELECT 
        n.N_NAME, 
        p.P_MFGR, 
        p.P_PARTKEY, 
        s.S_ACCTBAL, 
        s.S_ADDRESS, 
        s.S_NAME, 
        s.S_PHONE, 
        s.S_COMMENT, 
        n.N_NATIONKEY, 
        s.S_SUPPKEY 
    FROM 
        nation n 
    JOIN
        region r ON n.N_REGIONKEY = r.R_REGIONKEY 
    JOIN 
        supplier s ON s.S_NATIONKEY = n.N_NATIONKEY 
    JOIN 
        part p ON p.P_TYPE = 'BRASS' AND p.P_SIZE = 15 
    WHERE 
        r.R_NAME = 'EUROPE'
""")
mysql_data = mysql_cursor.fetchall()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_tpch = mongo_client["tpch"]
partsupp = mongo_tpch["partsupp"]

# Process data and write to CSV
with open('query_output.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    header = ['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']
    csv_writer.writerow(header)

    for row in mysql_data:
        n_name, p_mfgr, p_partkey, s_acctbal, s_address, s_name, s_phone, s_comment, n_nationkey, s_suppkey = row

        partsupp_docs = list(partsupp.find({
            "PS_PARTKEY": p_partkey,
            "PS_SUPPKEY": s_suppkey
        }).sort([("PS_SUPPLYCOST", pymongo.ASCENDING), ("S_ACCTBAL", pymongo.DESCENDING)]))

        if partsupp_docs:
            min_cost_doc = partsupp_docs[0]
            if min_cost_doc:
                csv_writer.writerow([n_name, p_mfgr, p_partkey, s_acctbal, s_address, s_comment, s_name, s_phone])

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
