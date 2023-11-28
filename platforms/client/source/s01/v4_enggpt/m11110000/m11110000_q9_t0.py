import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
nation_collection = mongodb["nation"]
part_collection = mongodb["part"]
supplier_collection = mongodb["supplier"]

# Query MongoDB for parts with 'dim' in the name
dim_parts_keys = [p['P_PARTKEY'] for p in part_collection.find({"P_NAME": {"$regex": "dim", "$options": "i"}})]

# Prepare MySQL queries
mysql_lineitem_query = """
SELECT
    L_ORDERKEY,
    L_PARTKEY,
    L_SUPPKEY,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_TAX,
    PS_SUPPLYCOST,
    O_ORDERDATE,
    O_NATIONKEY
FROM lineitem
JOIN partsupp ON lineitem.L_PARTKEY = partsupp.PS_PARTKEY AND lineitem.L_SUPPKEY = partsupp.PS_SUPPKEY
JOIN orders ON lineitem.L_ORDERKEY = orders.O_ORDERKEY
WHERE L_PARTKEY IN (%s)
"""
in_p = ', '.join(['%s'] * len(dim_parts_keys))
mysql_lineitem_query = mysql_lineitem_query % in_p

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_lineitem_query, dim_parts_keys)
    mysql_lineitem_results = cursor.fetchall()

# Fetch nation names from MongoDB
nations_dict = {n['N_NATIONKEY']: n['N_NAME'] for n in nation_collection.find()}

# Process results and write to CSV
with open("query_output.csv", "w", newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["Nation", "Year", "Profit"])
    for result in mysql_lineitem_results:
        order_nation = nations_dict.get(result[9])
        if not order_nation:  # If there's no nation key, skip the entry
            continue
        year = datetime.strptime(result[8], "%Y-%m-%d").year
        supply_cost = result[7]
        extended_price = result[4]
        discount = result[5]
        quantity = result[3]
        
        # Calculate profit
        profit = (extended_price * (1 - discount)) - (supply_cost * quantity)
        
        csvwriter.writerow([order_nation, year, profit])

# Close connections
mysql_conn.close()
mongo_client.close()
