from pymongo import MongoClient
import MySQLdb
import csv

# Connect to the MySQL Database
db = MySQLdb.connect(host="mysql", user="root", passwd="my-secret-pw", db="tpch")

# Create a cursor for the MySQL connection
mysql_cur = db.cursor()

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Create the MongoDB queries
suppliers_cursor = mongodb['supplier'].find({}, {
    "_id": 0,
    "S_SUPPKEY": 1,
    "S_NAME": 1,
    "S_ADDRESS": 1,
    "S_NATIONKEY": 1
})
lineitems_cursor = mongodb['lineitem'].aggregate([
    {"$match": {"L_SHIPDATE": {"$gte": "1994-01-01", "$lt": "1995-01-01"}}},
    {"$group": {"_id": {"L_PARTKEY": "$L_PARTKEY", "L_SUPPKEY": "$L_SUPPKEY"}, "total": {"$sum": "$L_QUANTITY"}}}
])

# Convert MongoDB cursors to lists
suppliers = list(suppliers_cursor)
lineitems = list(lineitems_cursor)

# Create the MySQL queries and execute them
mysql_cur.execute("SELECT * FROM part WHERE P_NAME LIKE 'forest%'")
parts = mysql_cur.fetchall()

mysql_cur.execute("SELECT * FROM partsupp")
part_supps = mysql_cur.fetchall()

mysql_cur.execute("SELECT * FROM nation WHERE N_NATIONKEY='CANADA'")
nations = mysql_cur.fetchall()

# Process the query logic
query_results = []
for supplier in suppliers:
    supp_key = supplier['S_SUPPKEY']
    nation_key = supplier['S_NATIONKEY']

    nation_name = None
    for nation in nations:
        if nation[0] == nation_key:
            nation_name = nation[1]
            break

    if nation_name != 'CANADA':
        continue

    for part in parts:
        part_key = part[0]

        for part_supp in part_supps:
            if supp_key == part_supp[1] and part_key == part_supp[0]:

                for lineitem in lineitems:
                    if lineitem['_id']['L_SUPPKEY'] == supp_key and lineitem['_id']['L_PARTKEY'] == part_key:
                        if part_supp[2] > (0.5 * lineitem['total']):
                            query_results.append([supplier['S_NAME'], supplier['S_ADDRESS']])

# Write the results to a CSV file
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(query_results)
