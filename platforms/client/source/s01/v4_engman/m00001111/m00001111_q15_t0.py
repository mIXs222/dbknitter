import pymongo
import pymysql
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Get supplier information from MySQL
mysql_cursor.execute("SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier")
suppliers = {row[0]: row[1:] for row in mysql_cursor.fetchall()}

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Query lineitem in MongoDB for the relevant date range and calculate revenue
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)
pipeline = [
    {'$match': {
        'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
    }},
    {'$group': {
        '_id': '$L_SUPPKEY',
        'TOTAL_REVENUE': {
            '$sum': {
                '$multiply': [
                    '$L_EXTENDEDPRICE',
                    {'$subtract': [1, '$L_DISCOUNT']}
                ]
            }
        }
    }},
    {'$sort': {'TOTAL_REVENUE': -1}}
]
lineitem_results = list(lineitem_collection.aggregate(pipeline))

# Join and find the maximum revenue
if lineitem_results:
    max_revenue = lineitem_results[0]['TOTAL_REVENUE']
    top_suppliers = [result for result in lineitem_results if result['TOTAL_REVENUE'] == max_revenue]

# Combine data and write to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
    for supplier in top_suppliers:
        s_suppkey = supplier['_id']
        if s_suppkey in suppliers:
            s_name, s_address, s_phone = suppliers[s_suppkey]
            writer.writerow([s_suppkey, s_name, s_address, s_phone, supplier['TOTAL_REVENUE']])

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
