from pymongo import MongoClient
import pymysql
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['lineitem']

# MySQL query to retrieve all parts that are promotional
mysql_cursor = mysql_conn.cursor()
mysql_query = """
SELECT P_PARTKEY
FROM part
WHERE P_NAME LIKE 'PROMO%'
"""
mysql_cursor.execute(mysql_query)
promotional_parts = set([row[0] for row in mysql_cursor.fetchall()])

# Mongo query to retrieve revenue from promotional parts within the date range
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)
lineitems_cursor = mongo_collection.find(
    {
        'L_SHIPDATE': {'$gte': start_date, '$lt': end_date},
        'L_PARTKEY': {'$in': list(promotional_parts)}
    },
    {
        '_id': 0, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1
    }
)

# Calculate revenue and write to query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    
    total_revenue = 0
    for lineitem in lineitems_cursor:
        revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
        total_revenue += revenue
    
    writer.writerow({'REVENUE': total_revenue})

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
