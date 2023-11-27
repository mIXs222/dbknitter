import pymysql
import pymongo
import csv
from datetime import datetime

# Connection to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connection to the MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Get all promotional parts from MySQL
mysql_cursor.execute("SELECT P_PARTKEY FROM part")
promotional_parts_set = {row[0] for row in mysql_cursor.fetchall()}

# Query the lineitems from MongoDB
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)
lineitems = lineitem_collection.find({
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
})

# Calculate the revenue contribution of promotional parts
total_revenue = 0.0
promo_revenue = 0.0
for lineitem in lineitems:
    revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    total_revenue += revenue
    if lineitem['L_PARTKEY'] in promotional_parts_set:
        promo_revenue += revenue

# Calculate the percentage
if total_revenue > 0:
    promo_percentage = (promo_revenue / total_revenue) * 100
else:
    promo_percentage = 0

# Write output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Promotion Effect Percentage'])
    csvwriter.writerow([promo_percentage])

# Close database connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
