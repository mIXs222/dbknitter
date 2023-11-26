import pymysql
import pymongo
import csv
import datetime

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
# MongoDB connection
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']

# Collect parts from MySQL where P_TYPE like 'PROMO%'
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("""
    SELECT P_PARTKEY, P_TYPE
    FROM part
    WHERE P_TYPE LIKE 'PROMO%'
""")
promo_parts = {row[0] for row in mysql_cursor.fetchall()}

# Define the date range for L_SHIPDATE
shipdate_start = datetime.datetime(1995, 9, 1)
shipdate_end = datetime.datetime(1995, 10, 1)

# Fetch lineitems from MongoDB
lineitems = mongodb.lineitem.find({
    'L_SHIPDATE': {'$gte': shipdate_start, '$lt': shipdate_end}
})

# Calculate PROMO_REVENUE
promo_revenue = 0
total_revenue = 0
for lineitem in lineitems:
    if lineitem['L_PARTKEY'] in promo_parts:
        promo_revenue += lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    total_revenue += lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])

# Avoid division by zero
if total_revenue == 0:
    promo_revenue_percent = 0.0
else:
    promo_revenue_percent = 100.0 * promo_revenue / total_revenue

# Write the results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['PROMO_REVENUE'])
    csvwriter.writerow([promo_revenue_percent])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
