import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# MySQL query to get parts of BRAND#23 with MED BAG
mysql_cursor.execute("""
SELECT P_PARTKEY
FROM part
WHERE P_BRAND = 'BRAND#23' AND P_CONTAINER = 'MED BAG';
""")
part_results = mysql_cursor.fetchall()
part_keys = [row[0] for row in part_results]

# MongoDB aggregation to calculate average quantity for the identified parts
pipeline = [
    {"$match": {"L_PARTKEY": {"$in": part_keys}}},
    {"$group": {"_id": None, "avg_qty": {"$avg": "$L_QUANTITY"}}}
]
avg_result = list(lineitem_collection.aggregate(pipeline))
avg_qty = avg_result[0]['avg_qty'] if avg_result else 0

# MongoDB query to get total extendedprice for quantities less than 20% average
small_qty_threshold = 0.2 * avg_qty
pipeline = [
    {"$match": {
        "L_PARTKEY": {"$in": part_keys},
        "L_QUANTITY": {"$lt": small_qty_threshold}
    }},
    {"$group": {
        "_id": None,
        "total_loss": {"$sum": "$L_EXTENDEDPRICE"}
    }}
]
loss_result = list(lineitem_collection.aggregate(pipeline))
total_loss = loss_result[0]['total_loss'] if loss_result else 0

# Writing the output to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(['Average Yearly Loss'])
    # Assuming 7 years in the database
    csv_writer.writerow([total_loss / 7])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
