import pymysql
import pymongo
import csv
from datetime import datetime

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Query for MySQL
mysql_query = """
SELECT L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT
FROM lineitem
WHERE L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE <= '1995-09-30';
"""

mysql_cursor.execute(mysql_query)
lineitems = mysql_cursor.fetchall()

# Connection to MongoDB
mongodb_conn = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_conn['tpch']
part_collection = mongodb_db['part']

# Querying MongoDB to find all parts starting with 'PROMO'
promo_parts_cursor = part_collection.find({"P_TYPE": {"$regex": "^PROMO"}}, {"P_PARTKEY": 1})
promo_part_keys = [doc['P_PARTKEY'] for doc in promo_parts_cursor]

# Calculate promotional and total revenue
promo_revenue = 0
total_revenue = 0

for lineitem in lineitems:
    l_partkey, l_extendedprice, l_discount = lineitem
    adjusted_price = l_extendedprice * (1 - l_discount)
    total_revenue += adjusted_price
    
    if l_partkey in promo_part_keys:
        promo_revenue += adjusted_price

# Calculate the promotional revenue percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Writing output to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promo Revenue Percentage'])
    writer.writerow([format(promo_revenue_percentage, '.2f')])

# Closing database connections
mysql_cursor.close()
mysql_conn.close()
mongodb_conn.close()

print('The promotional revenue percentage has been successfully calculated and written to query_output.csv.')
