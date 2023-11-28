import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to the MySQL server
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to the MongoDB server
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query the 'nation' table in MongoDB to find nation key for 'CANADA'
canada_nation_key = mongo_db.nation.find_one({'N_NAME': 'CANADA'}, {'N_NATIONKEY': 1})

# Proceed if Canada's nation key is found
if canada_nation_key:
    suppliers_in_canada = list(mongo_db.supplier.find({'S_NATIONKEY': canada_nation_key['N_NATIONKEY']}, {'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1}))

    # Get supplier keys in Canada
    supplier_keys_in_canada = [supplier['S_SUPPKEY'] for supplier in suppliers_in_canada]
    
    # Define a MySQL cursor
    mysql_cursor = mysql_conn.cursor()

    # Query partsupp and part in MySQL to find part keys for parts whose names start with 'forest'
    part_keys_forest = []
    mysql_cursor.execute("SELECT PS_PARTKEY FROM partsupp WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%')")
    for row in mysql_cursor:
        part_keys_forest.append(row[0])

    # Find the threshold quantities using the lineitem table
    thresholds = {}
    for ps_partkey in part_keys_forest:
        mysql_cursor.execute(
            f"SELECT L_SUPPKEY, SUM(L_QUANTITY) * 0.5 as threshold FROM lineitem WHERE L_PARTKEY = {ps_partkey} AND L_SUPPKEY IN %s AND L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01' GROUP BY L_SUPPKEY", (supplier_keys_in_canada,))
        for row in mysql_cursor:
            thresholds[row[0]] = row[1]

    # Finding suppliers in Canada that meet the threshold condition from lineitem
    qualifying_suppliers = []
    for supp in suppliers_in_canada:
        if supp['S_SUPPKEY'] in thresholds:
            qualifying_suppliers.append(supp)

    # Sorting suppliers based on names
    qualifying_suppliers = sorted(qualifying_suppliers, key=lambda k: k['S_NAME'])

    # Write the result to query_output.csv
    with open('query_output.csv', mode='w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(['S_NAME', 'S_ADDRESS'])  # Header
        for supplier in qualifying_suppliers:
            csv_writer.writerow([supplier['S_NAME'], supplier['S_ADDRESS']])

    # Close the MySQL cursor and connection
    mysql_cursor.close()
    mysql_conn.close()

# Close the MongoDB connection
mongo_client.close()
