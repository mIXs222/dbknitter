import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor  # Use the default tuple cursor
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Get the parts with P_TYPE starting with 'PROMO' from MongoDB
promo_parts_cursor = part_collection.find(
    {"P_TYPE": {"$regex": '^PROMO'}}
)
promo_parts = {doc['P_PARTKEY']: doc for doc in promo_parts_cursor}

# MySQL query to retrieve lineitem data for matching parts
query = """
SELECT 
    L_PARTKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT
FROM 
    lineitem
WHERE
    L_SHIPDATE >= %s
    AND L_SHIPDATE < %s
"""

# Set the date range for the query
start_date = datetime(1995, 9, 1).date()
end_date = datetime(1995, 10, 1).date()

# Execute query
cursor = mysql_conn.cursor()
cursor.execute(query, (start_date, end_date))

# Variables for calculating PROMO_REVENUE
promo_revenue = 0.0
total_revenue = 0.0

# Iterate through results and calculate promo revenue
for L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT in cursor:
    if L_PARTKEY in promo_parts:
        promo_revenue += L_EXTENDEDPRICE * (1 - L_DISCOUNT)
    total_revenue += L_EXTENDEDPRICE * (1 - L_DISCOUNT)

# Result
if total_revenue != 0:
    promo_revenue_percent = (promo_revenue / total_revenue) * 100
else:
    promo_revenue_percent = None

# Write result to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['PROMO_REVENUE'])
    writer.writerow([promo_revenue_percent])

# Close connections
cursor.close()
mysql_conn.close()
mongo_client.close()
