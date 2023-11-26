import pymysql
import pymongo
import csv

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# MongoDB collections
collection_part = mongo_db["part"]
collection_nation = mongo_db["nation"]

# MySQL cursor
mysql_cursor = mysql_conn.cursor()

# Query to get parts with names starting with 'forest%'
part_documents = collection_part.find({"P_NAME": {"$regex": '^forest'}}, {"P_PARTKEY": 1})

part_keys = [doc['P_PARTKEY'] for doc in part_documents]

# Query to get supplier keys from the partsupp table in MySQL
query_partsupp = """
SELECT PS_SUPPKEY
FROM partsupp
WHERE PS_PARTKEY IN ({}) AND PS_AVAILQTY > (
    SELECT 0.5 * SUM(L_QUANTITY)
    FROM lineitem
    WHERE
        L_PARTKEY = partsupp.PS_PARTKEY
        AND L_SUPPKEY = partsupp.PS_SUPPKEY
        AND L_SHIPDATE >= '1994-01-01'
        AND L_SHIPDATE < '1995-01-01'
)
""".format(",".join(str(k) for k in part_keys))

mysql_cursor.execute(query_partsupp)
supplier_keys = [row[0] for row in mysql_cursor.fetchall()]

# MongoDB query to get nationkey for 'CANADA'
nation_doc = collection_nation.find_one({"N_NAME": "CANADA"}, {"N_NATIONKEY": 1})
nation_key = nation_doc['N_NATIONKEY']

# Query to get supplier names and addresses from MySQL
query_supplier = """
SELECT S_NAME, S_ADDRESS 
FROM supplier
WHERE S_SUPPKEY IN ({}) AND S_NATIONKEY = %s
ORDER BY S_NAME
""".format(",".join(str(k) for k in supplier_keys))

mysql_cursor.execute(query_supplier, (nation_key,))
suppliers = mysql_cursor.fetchall()

# Write results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_NAME', 'S_ADDRESS'])
    writer.writerows(suppliers)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
