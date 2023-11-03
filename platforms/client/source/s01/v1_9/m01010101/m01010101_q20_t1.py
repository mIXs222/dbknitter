import csv
import mysql.connector
import pymongo

# create connection to mysql
mysql_conn = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
mysql_cursor = mysql_conn.cursor()

# run the sub-queries in MYSQL
mysql_cursor.execute("SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'")
partkeys = [item[0] for item in mysql_cursor.fetchall()]

mysql_cursor.execute("""SELECT SUM(L_QUANTITY)
                         FROM lineitem
                         WHERE L_PARTKEY IN (%s)
                            AND L_SHIPDATE >= '1994-01-01'
                            AND L_SHIPDATE < '1995-01-01'""" % ','.join(partkeys))
sum_quantity = mysql_cursor.fetchone()[0]

mysql_cursor.execute("""SELECT PS_SUPPKEY
                         FROM partsupp
                         WHERE PS_PARTKEY IN (%s)
                         AND PS_AVAILQTY > %s""" % (','.join(partkeys), str(sum_quantity/2)))
suppkeys = [item[0] for item in mysql_cursor.fetchall()]

# Create connection to MongoDB
mongo_conn = pymongo.MongoClient("mongodb://localhost:27017/")
mongodb = mongo_conn["tpch"]

# Query the MongoDB 'supplier' collection based on the supp keys found from MYSQL
suppliers = mongodb.supplier.find({"S_SUPPKEY": {"$in": suppkeys}})

# Query the MongoDB 'nation' collection
nations = list(mongodb.nation.find({"N_NATIONKEY": "CANADA"}))
nationkeys = [n["N_NATIONKEY"] for n in nations]

# write to csv
with open("query_output.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["S_NAME", "S_ADDRESS"])
    for s in suppliers:
        if s['S_NATIONKEY'] in nationkeys:
            writer.writerow([s["S_NAME"], s["S_ADDRESS"]])

print('Query output written to query_output.csv')
