# promotion_effect_query.py

import pymysql
import pymongo
import csv
from datetime import datetime

# Connection information for MySQL
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Connection information for MongoDB
mongo_conn_info = {
    'host': 'mongodb',
    'port': 27017,
    'db': 'tpch',
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_conn = pymongo.MongoClient(host=mongo_conn_info['host'], port=mongo_conn_info['port'])
mongo_db = mongo_conn[mongo_conn_info['db']]

# Fetch promotional parts from MySQL
promo_parts_sql = """
SELECT P_PARTKEY
FROM part
WHERE P_NAME LIKE 'PROMO%'
"""
mysql_cursor.execute(promo_parts_sql)
promo_parts = [row[0] for row in mysql_cursor.fetchall()]

# Fetch lineitem data from MongoDB within the specified date range
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)
lineitems = mongo_db.lineitem.find({
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date},
    'L_PARTKEY': {'$in': promo_parts}
})

# Calculate revenue for promotional parts
total_revenue = 0.0
for lineitem in lineitems:
    revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    total_revenue += revenue

# Write query result to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Total_Revenue'])
    csvwriter.writerow([total_revenue])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_conn.close()
