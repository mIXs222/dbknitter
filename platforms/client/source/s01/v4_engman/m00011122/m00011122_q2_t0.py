import pymysql
import pymongo
import csv

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Query MySQL to get information from 'nation' and 'part' tables
mysql_cursor = mysql_conn.cursor()
mysql_query = """
    SELECT n.N_NAME, p.P_MFGR, p.P_PARTKEY
    FROM nation n, region r, part p
    WHERE n.N_REGIONKEY = r.R_REGIONKEY AND r.R_NAME = 'EUROPE'
    AND p.P_TYPE = 'BRASS' AND p.P_SIZE = 15
"""
mysql_cursor.execute(mysql_query)
mysql_parts = mysql_cursor.fetchall()

# Query MongoDB to get information from 'supplier' and 'partsupp' collections
suppliers = list(mongodb['supplier'].find({'S_NATIONKEY': {'$in': [doc[2] for doc in mysql_parts]}}))
partsupp = list(mongodb['partsupp'].find({'PS_PARTKEY': {'$in': [doc[2] for doc in mysql_parts]}}))

# Join the data
result = []
for m_part in mysql_parts:
    for part_supp in partsupp:
        if part_supp['PS_PARTKEY'] == m_part[2]:  # Match part key
            for supplier in suppliers:
                if supplier['S_SUPPKEY'] == part_supp['PS_SUPPKEY']:  # Match supplier key
                    result.append((m_part[0], m_part[1], m_part[2], supplier['S_ACCTBAL'],
                                   supplier['S_ADDRESS'], supplier['S_COMMENT'], supplier['S_NAME'],
                                   supplier['S_PHONE'], part_supp['PS_SUPPLYCOST']))

# Sort the final results as per the requirement
sorted_result = sorted(result, key=lambda x: (-x[3], x[0], x[2], x[6]))

# Write the results to 'query_output.csv'
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE'])
    for row in sorted_result:
        csvwriter.writerow(row[:8])  # Exclude PS_SUPPLYCOST from the output

# Clean up connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
