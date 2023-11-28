import csv
import pymysql
import pymongo

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Retrieve region 'EUROPE' regionkey from MongoDB
europe_regionkey = mongodb_db.region.find_one({'R_NAME': 'EUROPE'})['R_REGIONKEY'] if mongodb_db.region.find_one({'R_NAME': 'EUROPE'}) else None

# Execute MySQL query
mysql_query = """
SELECT 
  s.S_ACCTBAL, n.N_NAME, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_COMMENT, p.P_PARTKEY, p.P_MFGR, p.P_SIZE 
FROM 
  nation n 
JOIN 
  supplier s ON n.N_NATIONKEY = s.S_NATIONKEY 
JOIN 
  partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY 
JOIN 
  part p ON ps.PS_PARTKEY = p.P_PARTKEY
WHERE 
  n.N_REGIONKEY = %s AND 
  p.P_SIZE = 15 AND 
  p.P_TYPE LIKE '%BRASS'
ORDER BY 
  s.S_ACCTBAL DESC, n.N_NAME ASC, s.S_NAME ASC, p.P_PARTKEY ASC
"""
mysql_cursor.execute(mysql_query, (europe_regionkey,))

# Write results to CSV file
with open('query_output.csv', mode='w') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['S_ACCTBAL', 'N_NAME', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE'])
    for row in mysql_cursor:
        csv_writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
