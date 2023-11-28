# query.py

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
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Date format
date_format = "%Y-%m-%d"

# Query MySQL for part types that start with 'PROMO'
mycursor = mysql_conn.cursor()
mycursor.execute("SELECT P_PARTKEY FROM part WHERE P_TYPE LIKE 'PROMO%'")
promo_parts = set(row[0] for row in mycursor.fetchall())

# Query MongoDB for lineitem data
lineitem_collection = mongodb_db['lineitem']

start_date = datetime.strptime('1995-09-01', date_format)
end_date = datetime.strptime('1995-09-30', date_format)

lineitems = lineitem_collection.find({
    'L_SHIPDATE': {'$gte': start_date, '$lte': end_date}
})

# Calculation
promo_revenue = 0
total_revenue = 0

for lineitem in lineitems:
    extended_price = lineitem['L_EXTENDEDPRICE']
    discount = lineitem['L_DISCOUNT']
    partkey = lineitem['L_PARTKEY']
    revenue = extended_price * (1 - discount)
    total_revenue += revenue
    if partkey in promo_parts:
        promo_revenue += revenue

# Calculate the percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Write to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promo Revenue Percentage'])
    writer.writerow([promo_revenue_percentage])

# Close connections
mycursor.close()
mysql_conn.close()
mongodb_client.close()
