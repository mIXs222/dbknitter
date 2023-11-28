import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch parts with brand 'Brand#23' and container type 'MED BAG' from MySQL
mysql_cursor.execute(
    "SELECT P_PARTKEY FROM part WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'"
)
part_keys = [row[0] for row in mysql_cursor.fetchall()]

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

# Fetch average quantities for the selected parts from MongoDB
avg_quantities = {}
for key in part_keys:
    avg_qty = lineitem_collection.aggregate([
        {"$match": {"L_PARTKEY": key}},
        {"$group": {"_id": "$L_PARTKEY", "avgQuantity": {"$avg": "$L_QUANTITY"}}}
    ])
    result = list(avg_qty)
    if result:
        avg_quantities[key] = result[0]['avgQuantity'] * 0.2

# Fetch line items that fulfill the average quantity condition from MongoDB
matching_lineitems = []
for part_key, qty_threshold in avg_quantities.items():
    lineitems = lineitem_collection.find({
        "L_PARTKEY": part_key,
        "L_QUANTITY": {"$lt": qty_threshold}
    }, {"L_EXTENDEDPRICE": 1, "L_QUANTITY": 1})
    for item in lineitems:
        item['yearly_avg'] = item['L_EXTENDEDPRICE'] / 7.0
        matching_lineitems.append(item)

# Writing to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Part Key', 'Extended Price', 'Yearly Avg'])
    for item in matching_lineitems:
        writer.writerow([item['L_PARTKEY'], item['L_EXTENDEDPRICE'], item['yearly_avg']])

# Cleanup and close connections
mysql_conn.close()
mongodb_client.close()
