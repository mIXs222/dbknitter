import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4'
)

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongodb_client['tpch']

# Find all suppliers from Germany
germany_suppliers = list(
    mongodb.nation.aggregate([
        {"$match": {"N_NAME": "GERMANY"}},
        {"$lookup": {
            "from": "supplier",
            "localField": "N_NATIONKEY",
            "foreignField": "S_NATIONKEY",
            "as": "suppliers"
        }},
        {"$unwind": "$suppliers"},
        {"$replaceRoot": {"newRoot": "$suppliers"}},
    ])
)

german_supplier_keys = [supp['S_SUPPKEY'] for supp in germany_suppliers]

# Query for MySQL database
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(
    "SELECT PS_PARTKEY, SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS total_value "
    "FROM partsupp "
    "WHERE PS_SUPPKEY IN (%s) "
    "GROUP BY PS_PARTKEY "
    "HAVING total_value > (SELECT 0.05 * SUM(PS_SUPPLYCOST * PS_AVAILQTY) FROM partsupp WHERE PS_SUPPKEY IN (%s))"
    % (','.join(['%s'] * len(german_supplier_keys)), ','.join(['%s'] * len(german_supplier_keys))),
    german_supplier_keys * 2
)
results = mysql_cursor.fetchall()

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['PS_PARTKEY', 'TOTAL_VALUE'])
    for row in results:
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
