# Python code to execute the query on original data
import pymysql
import pymongo
import csv
from datetime import datetime

# Convert shipping date to datetime
def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

# Query lineitems within the specified timeframe from MongoDB
start_date = parse_date("1995-09-01")
end_date = parse_date("1995-09-30")

lineitems = list(lineitem_collection.find({'L_SHIPDATE': {'$gte': start_date, '$lte': end_date}}))
lineitem_ids = [item['L_PARTKEY'] for item in lineitems]

# Query parts with 'PROMO' types from MySQL
promo_query = "SELECT P_PARTKEY FROM part WHERE P_TYPE LIKE 'PROMO%'"
mysql_cursor.execute(promo_query)
promo_parts = [row[0] for row in mysql_cursor.fetchall()]

# Close all database connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()

# Calculate promotional revenue and total revenue
promo_revenue = 0
total_revenue = 0
for lineitem in lineitems:
    extended_price = lineitem['L_EXTENDEDPRICE']
    discount = lineitem['L_DISCOUNT']
    discounted_price = extended_price * (1 - discount)
    
    if lineitem['L_PARTKEY'] in promo_parts:
        promo_revenue += discounted_price
    
    total_revenue += discounted_price

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promotional Revenue', 'Total Revenue', 'Promotional Revenue (%)'])
    
    if total_revenue > 0:
        promo_revenue_percentage = (promo_revenue / total_revenue) * 100
    else:
        promo_revenue_percentage = 0
    
    writer.writerow([promo_revenue, total_revenue, promo_revenue_percentage])
