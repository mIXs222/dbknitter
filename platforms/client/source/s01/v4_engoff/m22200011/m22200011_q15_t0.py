import csv
import pymysql
import pymongo
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
lineitem_collection = mongodb['lineitem']

# Query to get supplier revenue from MySQL
mysql_query = """
SELECT S_SUPPKEY, SUM(S_ACCTBAL) as total_revenue FROM supplier
GROUP BY S_SUPPKEY
"""

# Execute the MySQL query
mysql_cursor.execute(mysql_query)
supplier_revenues = {row[0]: row[1] for row in mysql_cursor.fetchall()}
mysql_cursor.close()

# Create datetime objects for the date range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)

# Get the lineitem data from MongoDB
lineitem_docs = lineitem_collection.find({
    "L_SHIPDATE": {"$gte": start_date, "$lt": end_date}
})

# Calculate the total revenue for each supplier and add it to MySQL results
for doc in lineitem_docs:
    supplier_key = doc['L_SUPPKEY']
    extended_price = doc['L_EXTENDEDPRICE']
    discount = doc['L_DISCOUNT']
    revenue_contribution = extended_price * (1 - discount)
    
    if supplier_key in supplier_revenues:
        supplier_revenues[supplier_key] += revenue_contribution

# Find the maximum revenue
max_revenue = max(supplier_revenues.values())
# Find top suppliers
top_suppliers = [k for k, v in supplier_revenues.items() if v == max_revenue]

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['S_SUPPKEY', 'Total_Revenue'])
    for supplier in sorted(top_suppliers):
        writer.writerow([supplier, supplier_revenues[supplier]])

# Close the connection to the MySQL database
mysql_conn.close()
