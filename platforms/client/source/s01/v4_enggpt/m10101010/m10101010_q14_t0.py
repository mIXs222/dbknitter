import pymysql
import pymongo
import csv
from datetime import datetime

# Establish MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Establish MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
part_col = mongodb['part']

# Fetch lineitem data within the specified timeframe
start_date = datetime.strptime('1995-09-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-09-30', '%Y-%m-%d')
query = """
SELECT
    L_PARTKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT
FROM
    lineitem
WHERE
    L_SHIPDATE BETWEEN %s AND %s
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(query, (start_date, end_date))
    lineitems = cursor.fetchall()

# Fetch part data with a type starting with 'PROMO'
promo_parts = list(part_col.find({"P_TYPE": {"$regex": r'^PROMO'}}))

# Process and calculate the results
promo_parts_keys = [x['P_PARTKEY'] for x in promo_parts]
promo_revenue = 0
total_revenue = 0

for lineitem in lineitems:
    extended_price = lineitem[1]
    discount = lineitem[2]
    revenue = extended_price * (1 - discount)
    total_revenue += revenue
    if lineitem[0] in promo_parts_keys:
        promo_revenue += revenue

# Compute promotional revenue percentage
promotional_revenue_percentage = 0
if total_revenue > 0:
    promotional_revenue_percentage = (promo_revenue / total_revenue) * 100

# Output the results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promotional Revenue Percentage'])
    writer.writerow([promotional_revenue_percentage])

# Close the connections
mysql_conn.close()
mongo_client.close()
