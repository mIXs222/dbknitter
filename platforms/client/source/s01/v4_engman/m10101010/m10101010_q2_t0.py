import pymysql
import pymongo
import csv

# MySQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB Connection
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']

# Query MySQL to get the Europe regionkey
mysql_cursor.execute("SELECT R_REGIONKEY FROM region WHERE R_NAME='EUROPE'")
europe_regionkey = mysql_cursor.fetchone()[0]

# Query MongoDB nations to get all nationkeys that belong to Europe
nations = list(mongodb.nation.find({'N_REGIONKEY': europe_regionkey}, {'N_NATIONKEY': 1}))

# Extract the nationkeys from nations
nationkeys = [n['N_NATIONKEY'] for n in nations]

# Query MongoDB parts to find all partkeys of parts that are of BRASS type and size of 15
parts = list(mongodb.part.find({'P_TYPE': 'BRASS', 'P_SIZE': 15}, {'P_PARTKEY': 1, 'P_MFGR': 1}))

# Extract the partkeys and manufacturing details from parts
partkeys = [(p['P_PARTKEY'], p['P_MFGR']) for p in parts]

# Query MySQL suppliers for the relevant suppliers
suppliers = []
for partkey, mfgr in partkeys:
    mysql_cursor.execute(f"""
        SELECT S.S_ACCTBAL, S.S_NAME, S.S_ADDRESS, S.S_PHONE, S.S_COMMENT, N.N_NAME
        FROM supplier S
        JOIN nation N ON S.S_NATIONKEY = N.N_NATIONKEY
        WHERE S.S_NATIONKEY IN ({','.join(map(str, nationkeys))})
        AND EXISTS (
            SELECT 1 FROM partsupp PS
            WHERE PS.PS_SUPPKEY = S.S_SUPPKEY
            AND PS.PS_PARTKEY = {partkey}
            ORDER BY PS.PS_SUPPLYCOST LIMIT 1
        )
    """)
    for s_acctbal, s_name, s_address, s_phone, s_comment, n_name in mysql_cursor.fetchall():
        suppliers.append({
            'N_NAME': n_name,
            'P_MFGR': mfgr,
            'P_PARTKEY': partkey,
            'S_ACCTBAL': s_acctbal,
            'S_ADDRESS': s_address,
            'S_COMMENT': s_comment,
            'S_NAME': s_name,
            'S_PHONE': s_phone
        })

# Sorting the result
suppliers.sort(key=lambda x: (-x['S_ACCTBAL'], x['N_NAME'], x['S_NAME'], x['P_PARTKEY']))

# Writing to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for supplier in suppliers:
        writer.writerow(supplier)

# Closing connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
