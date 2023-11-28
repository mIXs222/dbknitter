# query_code.py
import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Start and end dates
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)

# Query MongoDB for the lineitem data
lineitem_data = mongodb.lineitem.find({
    'L_SHIPDATE':{'$gte':start_date, '$lte':end_date}
}, {
    'L_SUPPKEY':1,
    'L_EXTENDEDPRICE':1,
    'L_DISCOUNT':1,
})

# Process the data and calculate the revenue for each supplier
supplier_revenue = {}
for lineitem in lineitem_data:
    supplier_no = lineitem['L_SUPPKEY']
    extended_price = lineitem['L_EXTENDEDPRICE']
    discount = lineitem['L_DISCOUNT']
    revenue = extended_price * (1 - discount)
    
    if supplier_no in supplier_revenue:
        supplier_revenue[supplier_no] += revenue
    else:
        supplier_revenue[supplier_no] = revenue

# Find the maximum revenue among suppliers
max_revenue_supplier = max(supplier_revenue, key=supplier_revenue.get)

# Query MySQL for the supplier data
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE
        FROM supplier
    """)
    suppliers_info = cursor.fetchall()

# Select the supplier with the maximum revenue
supplier_details = next((s for s in suppliers_info if str(s[0]) == max_revenue_supplier), None)
result = supplier_details + (supplier_revenue[max_revenue_supplier],) if supplier_details else None

# Write the output to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'REVENUE'])
    if result:
        writer.writerow(result)

# Close connections
mysql_conn.close()
mongo_client.close()
