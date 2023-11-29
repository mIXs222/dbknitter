# VolumeShippingQuery.py
import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = client['tpch']

# Constants for year filtering
start_date_1995 = datetime(1995, 1, 1)
end_date_1996 = datetime(1996, 12, 31)

# Get nation keys for INDIA and JAPAN from MySQL nations table.
nation_keys = {'INDIA': None, 'JAPAN': None}
mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME IN ('INDIA', 'JAPAN');")
for n_key, n_name in mysql_cursor.fetchall():
    nation_keys[n_name] = n_key

# Performing query on MongoDB
pipeline = [
    {"$match": {
        "L_SHIPDATE": {"$gte": start_date_1995, "$lt": end_date_1996},
        "$or": [
            {"L_SUPPKEY": {"$in": mongodb.supplier.find({"S_NATIONKEY": nation_keys['INDIA']}, {"S_SUPPKEY": 1})}},
            {"L_SUPPKEY": {"$in": mongodb.supplier.find({"S_NATIONKEY": nation_keys['JAPAN']}, {"S_SUPPKEY": 1})}}
        ]
    }},
    {"$lookup": {
        "from": "orders", 
        "localField": "L_ORDERKEY", 
        "foreignField": "O_ORDERKEY", 
        "as": "order_info"
    }},
    {"$unwind": "$order_info"},
    {"$lookup": {
        "from": "customer", 
        "localField": "order_info.O_CUSTKEY", 
        "foreignField": "C_CUSTKEY", 
        "as": "customer_info"
    }},
    {"$unwind": "$customer_info"},
    {"$project": {
        "L_YEAR": {"$year": "$L_SHIPDATE"},
        "REVENUE": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]},
        "CUST_NATION": "$customer_info.C_NATIONKEY",
        "SUPP_NATION": "$L_SUPPKEY",
        "_id": 0
    }}
]

# Execute pipeline
lineitems = mongodb.lineitem.aggregate(pipeline)

# Join the results from MongoDB with nation information from MySQL
results = []
for item in lineitems:
    # Get the nation names
    mysql_cursor.execute("SELECT N_NAME FROM nation WHERE N_NATIONKEY = %s", (item['SUPP_NATION'],))
    supp_nation_name = mysql_cursor.fetchone()[0]

    mysql_cursor.execute("SELECT N_NAME FROM nation WHERE N_NATIONKEY = %s", (item['CUST_NATION'],))
    cust_nation_name = mysql_cursor.fetchone()[0]

    # Filter nation names (INDIA-JAPAN and JAPAN-INDIA)
    if (supp_nation_name, cust_nation_name) == ('INDIA', 'JAPAN') or (supp_nation_name, cust_nation_name) == ('JAPAN', 'INDIA'):
        results.append((cust_nation_name, item['L_YEAR'], item['REVENUE'], supp_nation_name))

# Sorting the results
sorted_results = sorted(results, key=lambda x: (x[0], x[1], x[3]))

# Output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    filewriter = csv.writer(csvfile)
    filewriter.writerow(['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION'])
    for row in sorted_results:
        filewriter.writerow(row)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
