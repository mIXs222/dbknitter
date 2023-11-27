import pymysql
import pymongo
import csv

# Establish a connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Get parts that satisfy the conditions from MongoDB
satisfying_parts = list(mongodb['part'].find({
    'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]},
    'P_TYPE': {'$ne': 'MEDIUM POLISHED'},
    'P_BRAND': {'$ne': 'Brand#45'}
}, {'_id': 0, 'P_PARTKEY': 1}))

part_keys = [part['P_PARTKEY'] for part in satisfying_parts]

# Construct a MySQL query to find suppliers without using NOT EXISTS
supplier_query = f"""
SELECT
  PS_SUPPKEY,
  COUNT(DISTINCT PS_PARTKEY) AS PART_COUNT
FROM
  partsupp
WHERE
  PS_PARTKEY IN ({','.join(['%s'] * len(part_keys))})
  AND PS_SUPPKEY NOT IN (
    SELECT S_SUPPKEY
    FROM supplier
    WHERE S_COMMENT LIKE '%%Customer%%Complaints%%'
  )
GROUP BY PS_SUPPKEY
ORDER BY PART_COUNT DESC, S_NAME ASC;
"""

# Execute the MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(supplier_query, part_keys)
    supplier_results = cursor.fetchall()

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['SUPPLIER_COUNT'])  # Write the header
    for result in supplier_results:
        writer.writerow(result)  # Write each result row

# Close the connections
mysql_conn.close()
mongo_client.close()
