# query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_lineitem = mongo_db["lineitem"]

# Filter dates for MongoDB query
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)

# Retrieve data from MongoDB
mongo_query = {
    "L_SHIPDATE": {"$gte": start_date, "$lt": end_date}
}
lineitems = list(mongo_lineitem.find(mongo_query, {"_id": 0, "L_PARTKEY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1}))

# Create a dictionary of partkeys to calculate revenue
part_revenue = {}
for item in lineitems:
    partkey = item["L_PARTKEY"]
    price = item["L_EXTENDEDPRICE"]
    discount = item["L_DISCOUNT"]
    revenue = price * (1 - discount)
    if partkey in part_revenue:
        part_revenue[partkey] += revenue
    else:
        part_revenue[partkey] = revenue

# Retrieve promotion part keys from MySQL
promotion_part_keys = []
mysql_cursor.execute("SELECT P_PARTKEY FROM part WHERE P_NAME LIKE '%Promo%';")
for row in mysql_cursor.fetchall():
    promotion_part_keys.append(row[0])

# Calculate the total revenue and promotion revenue
total_revenue = 0
promotion_revenue = 0
for partkey, revenue in part_revenue.items():
    total_revenue += revenue
    if partkey in promotion_part_keys:
        promotion_revenue += revenue

# Calculate the promotion revenue percentage
promotion_revenue_percentage = 0 if total_revenue == 0 else (promotion_revenue / total_revenue) * 100

# Save the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Promotion Revenue Percentage'])
    writer.writerow([promotion_revenue_percentage])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
