# query_script.py
import csv
import pymysql
import pymongo
from datetime import datetime

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
mongodb = mongo_client['tpch']

# Query MySQL
mysql_query = """
SELECT
    DATE_FORMAT(O_ORDERDATE, '%%Y') AS O_YEAR,
    L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME,
    C_NATIONKEY
FROM
    lineitem, orders, customer
WHERE
    L_ORDERKEY = O_ORDERKEY
    AND O_CUSTKEY = C_CUSTKEY
    AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
"""

# Query MongoDB for parts and nations
part_docs = mongodb['part'].find({'P_TYPE': 'SMALL PLATED COPPER'})
supplier_nation_docs = mongodb['nation'].find({'N_NAME': 'INDIA'})
region_docs = mongodb['nation'].find({'N_NAME': 'ASIA'})

# Extract part keys, nation keys, and ASIA region keys
part_keys = {doc['P_PARTKEY'] for doc in part_docs}
india_nation_keys = {doc['N_NATIONKEY'] for doc in supplier_nation_docs}
asia_region_keys = {doc['N_REGIONKEY'] for doc in region_docs}

# Execute MySQL query
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(mysql_query)
mysql_data = mysql_cursor.fetchall()

# Process the data
results = {}
for row in mysql_data:
    o_year, volume, c_nationkey = row
    if c_nationkey in india_nation_keys:
        nation = 'INDIA'
    else:
        nation = 'OTHER'

    if o_year not in results:
        results[o_year] = {'INDIA': 0, 'TOTAL': 0}
    
    results[o_year]['TOTAL'] += volume
    if nation == 'INDIA':
        results[o_year]['INDIA'] += volume

# Disconnect from MySQL and MongoDB
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_YEAR', 'MKT_SHARE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for o_year in sorted(results.keys()):
        if results[o_year]['TOTAL'] > 0:
            india_share = results[o_year]['INDIA'] / results[o_year]['TOTAL']
        else:
            india_share = 0
        writer.writerow({'O_YEAR': o_year, 'MKT_SHARE': india_share})
