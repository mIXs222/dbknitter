import pymongo
import pymysql
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]

# Perform subquery to get the minimum PS_SUPPLYCOST for Europe region
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT MIN(PS_SUPPLYCOST)
        FROM partsupp, supplier, nation, region
        WHERE S_SUPPKEY = PS_SUPPKEY
            AND S_NATIONKEY = N_NATIONKEY
            AND N_REGIONKEY = R_REGIONKEY
            AND R_NAME = 'EUROPE'
    """)
    min_supplycost = cursor.fetchone()[0]

# Main query
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT
            S_ACCTBAL,
            S_NAME,
            N_NAME,
            P_PARTKEY,
            P_MFGR,
            S_ADDRESS,
            S_PHONE,
            S_COMMENT
        FROM
            part, supplier, nation, region
        WHERE
            P_SIZE = 15
            AND P_TYPE LIKE '%BRASS'
            AND S_NATIONKEY = N_NATIONKEY
            AND N_REGIONKEY = R_REGIONKEY
            AND R_NAME = 'EUROPE'
    """)
    mysql_results = cursor.fetchall()

# Filter MongoDB partsupp collection by PS_SUPPLYCOST
partsupp_results = list(mongodb.partsupp.find(
    {"PS_SUPPLYCOST": {"$eq": min_supplycost}}, 
    {"_id": 0, "PS_PARTKEY": 1, "PS_SUPPKEY": 1}
))

# Mapping from (P_PARTKEY, S_SUPPKEY) to PS_SUPPLYCOST
partsupp_map = {(doc["PS_PARTKEY"], doc["PS_SUPPKEY"]): doc["PS_SUPPLYCOST"]
                for doc in partsupp_results}

# Final join and filter
final_results = [
    row for row in mysql_results
    if (row[3], row[0]) in partsupp_map  # (P_PARTKEY, S_SUPPKEY) from the MySQL result
]

# Sort the results
final_results.sort(key=lambda x: (-x[0], x[2], x[1], x[3]))  # S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'])
    csvwriter.writerows(final_results)

# Close connections
mysql_conn.close()
mongo_client.close()
