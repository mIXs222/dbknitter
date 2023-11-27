import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']

# Fetch all suppliers in GERMANY from MongoDB
germany_suppliers = mongo_db['nation'].find({"N_NAME": "GERMANY"})
germany_suppliers_keys = [doc['N_NATIONKEY'] for doc in germany_suppliers]

supplier_keys = mongo_db['supplier'].find(
    {"S_NATIONKEY": {"$in": germany_suppliers_keys}},
    {"S_SUPPKEY": 1}
)

supplier_keys_in_germany = [doc['S_SUPPKEY'] for doc in supplier_keys]

# Fetch part supplier data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute(
        "SELECT PS_PARTKEY, SUM(PS_AVAILQTY * PS_SUPPLYCOST) AS total_value "
        "FROM partsupp WHERE PS_SUPPKEY IN (%s) "
        "GROUP BY PS_PARTKEY HAVING total_value > 0.0001 "
        "ORDER BY total_value DESC", ','.join(map(str, supplier_keys_in_germany))
    )
    result = cursor.fetchall()

# Write query result to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PS_PARTKEY', 'TOTAL_VALUE'])
    for row in result:
        writer.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
