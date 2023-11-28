import pymysql
import pymongo
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
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# MySQL cursor
mysql_cursor = mysql_conn.cursor()

# Query MySQL for promo parts keys
promo_parts_keys = set()
query_mysql = """
SELECT P_PARTKEY FROM part WHERE P_TYPE LIKE 'PROMO%%'
"""
mysql_cursor.execute(query_mysql)
for row in mysql_cursor:
    promo_parts_keys.add(row[0])

# Define timeframe
start_date = datetime(1995, 9, 1).strftime('%Y-%m-%d')
end_date = datetime(1995, 9, 30).strftime('%Y-%m-%d')

# Query MongoDB for line items within timeframe
lineitems_cursor = mongo_db.lineitem.find({
    'L_SHIPDATE': {
        '$gte': start_date,
        '$lte': end_date
    }
})

# Variables for revenue calculation
promo_revenue = 0
total_revenue = 0

# Process line items and calculate revenues
for lineitem in lineitems_cursor:
    extended_price = lineitem['L_EXTENDEDPRICE']
    discount = lineitem['L_DISCOUNT']
    revenue = extended_price * (1 - discount)
    
    total_revenue += revenue
    if lineitem['L_PARTKEY'] in promo_parts_keys:
        promo_revenue += revenue

# Calculate promotional revenue percentage
promo_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Promotional Revenue Percentage'])
    writer.writerow([promo_percentage])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
