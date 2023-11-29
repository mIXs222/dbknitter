import csv
import pymysql
import pymongo
from datetime import datetime

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
mongo_part_collection = mongo_db['part']

# Get promotion part keys
promo_parts = mongo_part_collection.find({}, {'P_PARTKEY': 1, '_id': 0})
promo_part_keys = [part['P_PARTKEY'] for part in promo_parts]

# MySQL query
mysql_cursor = mysql_conn.cursor()
mysql_query = """
    SELECT
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS promo_revenue
    FROM
        lineitem
    WHERE
        L_SHIPDATE >= %s AND
        L_SHIPDATE < %s AND
        L_PARTKEY IN %s;
"""
start_date = datetime(1995, 9, 1).date()
end_date = datetime(1995, 10, 1).date()

mysql_cursor.execute(mysql_query, (start_date, end_date, promo_part_keys))
result = mysql_cursor.fetchone()
promo_revenue = result[0] if result else 0

# Total revenue
mysql_cursor.execute("""
    SELECT
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
    FROM
        lineitem
    WHERE
        L_SHIPDATE >= %s AND
        L_SHIPDATE < %s;
""", (start_date, end_date))
result = mysql_cursor.fetchone()
total_revenue = result[0] if result else 0

# Calculate percentage
percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Write output to csv
with open('query_output.csv', 'w', newline='') as outfile:
    csv_writer = csv.writer(outfile)
    csv_writer.writerow(['promo_revenue', 'total_revenue', 'percentage'])
    csv_writer.writerow([promo_revenue, total_revenue, percentage])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
