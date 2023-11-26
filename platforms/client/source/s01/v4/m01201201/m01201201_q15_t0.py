import pymysql
import pymongo
import csv
from datetime import datetime, timedelta

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client["tpch"]
lineitem_col = mongodb["lineitem"]

# Calculate the date range
start_date = datetime.strptime('1996-01-01', '%Y-%m-%d')
end_date = start_date + timedelta(months=3)

# MongoDB query for the revenue
pipeline = [
    {'$match': {
        'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
    }},
    {'$group': {
        '_id': "$L_SUPPKEY",
        'TOTAL_REVENUE': {'$sum': {'$multiply': ["$L_EXTENDEDPRICE", {'$subtract': [1, "$L_DISCOUNT"]}]}},
    }},
    {'$sort': {'TOTAL_REVENUE': -1}}
]

lineitem_result = list(lineitem_col.aggregate(pipeline))
max_revenue = lineitem_result[0]['TOTAL_REVENUE'] if lineitem_result else 0

supplier_no_list = [d['_id'] for d in lineitem_result if d['TOTAL_REVENUE'] == max_revenue]

# MySQL query for supplier details
supplier_cursor = mysql_conn.cursor()
supplier_cursor.execute("""
SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE
FROM supplier
WHERE S_SUPPKEY IN (%s)
""" % ','.join(['%s'] * len(supplier_no_list)), supplier_no_list)

suppliers = supplier_cursor.fetchall()

# Combine data
combined_results = [
    (s[0], s[1], s[2], s[3], max_revenue)
    for s in suppliers
    if any(s[0] == record['_id'] for record in lineitem_result)
]

# Write results to file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
    writer.writerows(combined_results)

# Close connections
supplier_cursor.close()
mysql_conn.close()
mongo_client.close()
