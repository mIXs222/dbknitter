import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_conn = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_conn['tpch']

# Retrieve data from MySQL
mysql_query = """
SELECT 
    n.N_NATIONKEY, n.N_NAME, n.N_REGIONKEY,
    r.R_REGIONKEY, r.R_NAME,
    p.P_PARTKEY, p.P_MFGR, p.P_SIZE, p.P_TYPE
FROM 
    nation n, region r, part p
WHERE 
    p.P_SIZE = 15 
    AND p.P_TYPE LIKE '%BRASS' 
    AND n.N_REGIONKEY = r.R_REGIONKEY 
    AND r.R_NAME = 'EUROPE'
"""
mysql_cursor.execute(mysql_query)
mysql_data = mysql_cursor.fetchall()

# Filter MongoDB documents for supplier and partsupp
suppliers = list(mongo_db.supplier.find({'S_NATIONKEY': {'$in': [row[0] for row in mysql_data]}}))
partsupp = mongo_db.partsupp.aggregate([
    {
        '$group': {
            '_id': '$PS_PARTKEY',
            'min_cost': {'$min': '$PS_SUPPLYCOST'}
        }
    }
])
min_cost_dict = {doc['_id']: doc['min_cost'] for doc in partsupp}

# Combine data from both sources
combined_data = []
for row in mysql_data:
    partkey, mfgr, nation_key = row[5], row[6], row[0]
    min_cost = min_cost_dict.get(partkey)
    
    if min_cost is not None:
        matching_suppliers = [
            supp for supp in suppliers 
            if supp['S_SUPPKEY'] in [ps['PS_SUPPKEY'] for ps in mongo_db.partsupp.find({'PS_PARTKEY': partkey, 'PS_SUPPLYCOST': min_cost})]
        ]
        
        for supp in matching_suppliers:
            combined_data.append([
                supp['S_ACCTBAL'],
                supp['S_NAME'],
                row[1],  # N_NAME
                partkey,
                mfgr,
                supp['S_ADDRESS'],
                supp['S_PHONE'],
                supp['S_COMMENT']
            ])

# Sort data according to the given query
combined_data.sort(key=lambda x: (-x[0], x[2], x[1], x[3]))

# Write output to 'query_output.csv'
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'])
    for data_row in combined_data:
        csvwriter.writerow(data_row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_conn.close()
