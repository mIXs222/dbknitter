# supplier_revenue_analysis.py
import pymysql
import pymongo
import csv
from datetime import datetime

# Establishing connections to mysql and mongodb
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mongodb_conn = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_conn['tpch']

# Getting data from MongoDB
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': datetime(1996, 1, 1),
                '$lte': datetime(1996, 3, 31)
            }
        }
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'TotalRevenue': {
                '$sum': {
                    '$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]
                }
            }
        }
    }
]

lineitem_data = list(mongodb_db.lineitem.aggregate(pipeline))

# Getting the max revenue supplier from lineitem_data
max_revenue_supplier = max(lineitem_data, key=lambda x: x['TotalRevenue'])

# Getting data from MySQL
mysql_cursor = mysql_conn.cursor()
mysql_query = """
SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE
FROM supplier
WHERE S_SUPPKEY = %s
ORDER BY S_SUPPKEY ASC;
"""
mysql_cursor.execute(mysql_query, (max_revenue_supplier['_id'], ))

# Writing query results to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    # Write headers
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TotalRevenue'])
    for supplier in mysql_cursor:
        writer.writerow(supplier + (max_revenue_supplier['TotalRevenue'], ))

# Closing connections
mysql_cursor.close()
mysql_conn.close()
mongodb_conn.close()
