import pymysql
import pymongo
import csv
from datetime import datetime

# Connecting to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()
mysql_query = """
SELECT P_PARTKEY
FROM part
WHERE P_NAME LIKE '%Promo%'
"""
mysql_cursor.execute(mysql_query)
promo_parts = [row[0] for row in mysql_cursor.fetchall()]
mysql_cursor.close()
mysql_conn.close()

# Connecting to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)
revenue = 0
promo_revenue = 0

for document in lineitem_collection.find({
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
}):
    extended_price = document['L_EXTENDEDPRICE']
    discount = document['L_DISCOUNT']
    order_revenue = extended_price * (1 - discount)
    if document['L_PARTKEY'] in promo_parts:
        promo_revenue += order_revenue
    revenue += order_revenue

percentage = (promo_revenue / revenue) * 100 if revenue else 0

# Writing to file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Promotion Revenue Percentage'])
    writer.writerow([percentage])

print("Query executed. Results are written to query_output.csv")
