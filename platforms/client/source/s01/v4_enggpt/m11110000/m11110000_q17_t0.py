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
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Get the average quantity for each part from the lineitem MySQL table
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT L_PARTKEY, AVG(L_QUANTITY) * 0.2 AS avg_20_quant 
        FROM lineitem 
        GROUP BY L_PARTKEY;
    """)
    avg_quantities = {row[0]: row[1] for row in cursor.fetchall()}

# Get relevant part data from MongoDB
parts = part_collection.find({"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"}, {"_id": 0, "P_PARTKEY": 1})

# Convert MongoDB cursor to list and then to set for faster lookup
part_keys = {doc["P_PARTKEY"] for doc in parts}

# Retrieve lineitem data based on part keys and average quantities
with mysql_conn.cursor() as cursor:
    placeholders = ', '.join(['%s'] * len(part_keys))
    query = f"""
        SELECT L_PARTKEY, SUM(L_EXTENDEDPRICE) / 7.0 AS avg_yearly_ext_price
        FROM lineitem
        WHERE L_PARTKEY IN ({placeholders})
          AND L_QUANTITY < %s
        GROUP BY L_PARTKEY;
    """
    param = list(part_keys) + [avg_quantities[part_key] for part_key in part_keys if part_key in avg_quantities]
    cursor.execute(query, param)
    result = cursor.fetchall()

# Write the query output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['P_PARTKEY', 'AVG_YEARLY_EXT_PRICE'])
    for row in result:
        writer.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
