import pymysql
import pymongo
import csv
import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
nation_collection = mongodb_db['nation']
supplier_collection = mongodb_db['supplier']

# Get the nationkey for CANADA
canada_nationkey = nation_collection.find_one({'N_NAME': 'CANADA'})['N_NATIONKEY']

# Get relevant suppliers for CANADA
suppliers_for_canada = supplier_collection.find({'S_NATIONKEY': canada_nationkey}, {'S_SUPPKEY': 1})

supplier_keys = [supplier['S_SUPPKEY'] for supplier in suppliers_for_canada]

# MySQL Query
mysql_query = """
SELECT 
    l.L_SUPPKEY,
    SUM(l.L_QUANTITY) AS total_quantity
FROM 
    lineitem l
WHERE 
    l.L_SUPPKEY IN (%s) AND 
    l.L_SHIPDATE >= '1994-01-01' AND 
    l.L_SHIPDATE < '1995-01-01'
GROUP BY 
    l.L_SUPPKEY
HAVING 
    SUM(l.L_QUANTITY) > (
        SELECT 
            0.5 * SUM(ps.PS_AVAILQTY)
        FROM 
            partsupp ps
        WHERE 
            ps.PS_SUPPKEY = l.L_SUPPKEY
    )
""" % ','.join(str(s) for s in supplier_keys)

mysql_cursor.execute(mysql_query)
suppliers_with_excess = mysql_cursor.fetchall()

# Write to file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['SUPPKEY', 'TOTAL_QUANTITY'])
    for row in suppliers_with_excess:
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
