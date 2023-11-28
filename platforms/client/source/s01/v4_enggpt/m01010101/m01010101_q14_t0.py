import pymysql
import pymongo
import csv
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
lineitem_collection = mongodb['lineitem']

# Dates for the specified timeframe
start_date = datetime.datetime(1995, 9, 1)
end_date = datetime.datetime(1995, 9, 30)

# Get part keys for promotional parts from MySQL
promo_part_keys = set()
mysql_cursor.execute("SELECT P_PARTKEY FROM part WHERE P_TYPE LIKE 'PROMO%'")
for row in mysql_cursor:
    promo_part_keys.add(row[0])

# Fetch line items within the date range from MongoDB
query = {
    'L_SHIPDATE': {'$gte': start_date, '$lte': end_date}
}
lineitems_cursor = lineitem_collection.find(query)

# Calculate promotional revenue and total revenue
promo_revenue = 0.0
total_revenue = 0.0
for lineitem in lineitems_cursor:
    extended_price_discounted = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    total_revenue += extended_price_discounted
    if lineitem['L_PARTKEY'] in promo_part_keys:
        promo_revenue += extended_price_discounted

# Calculate the percentage of promotional revenue
percentage_promo_revenue = (promo_revenue / total_revenue) * 100 if total_revenue != 0 else 0

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['PROMO_REVENUE_PERCENT'])
    writer.writerow([percentage_promo_revenue])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
