import pymysql
import pymongo
import csv
from pymongo import MongoClient
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
client = MongoClient('mongodb', 27017)
mongodb = client.tpch

start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)

# Fetch lineitem data from MongoDB filtered by the dates
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': start_date,
                '$lt': end_date
            }
        }
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'TOTAL_REVENUE': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE', 
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            }
        }
    },
    {
        '$sort': {'TOTAL_REVENUE': -1}
    },
]
lineitem_aggregation = mongodb.lineitem.aggregate(pipeline)

# Save the aggregated data temporarily
revenue_by_supplier = {}
for doc in lineitem_aggregation:
    revenue_by_supplier[doc['_id']] = doc['TOTAL_REVENUE']

# Find the maximum revenue
max_revenue = max(revenue_by_supplier.values()) if revenue_by_supplier else 0

# Fetch suppliers from MySQL
mysql_cursor.execute("SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier")
suppliers_data = mysql_cursor.fetchall()

# Combine the data and write to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])

    for supplier in suppliers_data:
        suppkey = supplier[0]
        if suppkey in revenue_by_supplier and revenue_by_supplier[suppkey] == max_revenue:
            writer.writerow(supplier + (revenue_by_supplier[suppkey],))

# Close connections
mysql_cursor.close()
mysql_conn.close()
client.close()
