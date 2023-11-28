import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cur = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Retrieve suppliers and nations from MySQL
mysql_cur.execute("SELECT s.S_NAME, s.S_ADDRESS, s.S_SUPPKEY FROM supplier s JOIN nation n ON s.N_NATIONKEY = n.N_NATIONKEY WHERE n.N_NAME = 'CANADA'")
suppliers = mysql_cur.fetchall()

# Retrieve parts with names starting with 'forest' from MongoDB
part_keys = mongodb['partsupp'].distinct('PS_PARTKEY', {'PS_COMMENT': {'$regex': '^forest'}})

# Calculate threshold quantity from MongoDB
threshold_quantities = {}
for part_key in part_keys:
    pipeline = [
        {'$match': {'L_PARTKEY': part_key, 'L_SHIPDATE': {'$gte': '1994-01-01', '$lte': '1995-01-01'}}},
        {'$group': {'_id': '$L_SUPPKEY', 'total_quantity': {'$sum': '$L_QUANTITY'}}}
    ]
    for doc in mongodb['lineitem'].aggregate(pipeline):
        threshold_quantities[(part_key, doc['_id'])] = doc['total_quantity'] / 2.0

# Find eligible suppliers and write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['S_NAME', 'S_ADDRESS'])

    for s_name, s_address, s_suppkey in suppliers:
        for part_key in part_keys:
            if (part_key, s_suppkey) in threshold_quantities and threshold_quantities[(part_key, s_suppkey)] > 0:
                writer.writerow([s_name, s_address])
                break

# Close connections
mysql_cur.close()
mysql_conn.close()
mongo_client.close()
