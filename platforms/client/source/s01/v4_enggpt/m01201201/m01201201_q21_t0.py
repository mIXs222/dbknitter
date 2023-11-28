import csv
import pymysql
import pymongo
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client['tpch']
lineitems = mongodb['lineitem']

def get_suppliers_from_nation(nation_name, mysql_conn):
    cursor = mysql_conn.cursor()
    cursor.execute(
        "SELECT S_SUPPKEY, S_NAME FROM supplier "
        "JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY "
        "WHERE nation.N_NAME = %s", (nation_name,)
    )
    suppliers = cursor.fetchall()
    cursor.close()
    return {suppkey: name for suppkey, name in suppliers}

def get_fulfilled_orders(mysql_conn):
    cursor = mysql_conn.cursor()
    cursor.execute("SELECT O_ORDERKEY FROM orders WHERE O_ORDERSTATUS = 'F'")
    orders = cursor.fetchall()
    cursor.close()
    return {row[0] for row in orders}

def get_lineitem_data(supplier_keys, fulfilled_orders, lineitems):
    waiting_times = {suppkey: 0 for suppkey in supplier_keys}
    for lineitem in lineitems.find({"L_SUPPKEY": {"$in": list(supplier_keys)}}):
        if (lineitem["L_ORDERKEY"] in fulfilled_orders and
                lineitem["L_RECEIPTDATE"] > lineitem["L_COMMITDATE"]):
            waiting_times[lineitem["L_SUPPKEY"]] += 1
    return waiting_times

# Get suppliers from Saudi Arabia
saudi_suppliers = get_suppliers_from_nation("SAUDI ARABIA", mysql_conn)

# Get fulfilled orders
fulfilled_orders = get_fulfilled_orders(mysql_conn)

# Get line items data
waiting_times = get_lineitem_data(set(saudi_suppliers.keys()), fulfilled_orders, lineitems)

# Combine data and sort by waiting time descending and supplier name ascending
combined_data = [(saudi_suppliers[suppkey], waiting_times[suppkey]) for suppkey in waiting_times]
combined_data.sort(key=lambda x: (-x[1], x[0]))

# Write output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['S_NAME', 'NUMWAIT'])
    csvwriter.writerows(combined_data)

# Close connections
mysql_conn.close()
client.close()
