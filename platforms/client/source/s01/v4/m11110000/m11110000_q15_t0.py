import csv
import pymysql
import pymongo
from datetime import datetime, timedelta

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cur = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch
supplier_collection = mongodb.supplier

# SQL Query for revenue0 (lineitem table)
mysql_query = """
SELECT
  L_SUPPKEY as SUPPLIER_NO,
  SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
FROM
  lineitem
WHERE
  L_SHIPDATE >= '1996-01-01'
  AND L_SHIPDATE < %s
GROUP BY
  L_SUPPKEY
"""

end_date = datetime(1996, 1, 1) + timedelta(days=90)
end_date_str = end_date.strftime('%Y-%m-%d')
mysql_cur.execute(mysql_query, (end_date_str,))

revenue0_results = mysql_cur.fetchall()

# Get the max TOTAL_REVENUE from the results
max_total_revenue = max(revenue0_results, key=lambda x: x[1])[1]

# Get supplier details from MongoDB
supplier_with_max_revenue = supplier_collection.find(
    {"S_SUPPKEY": {"$in": [row[0] for row in revenue0_results if row[1] == max_total_revenue]}}
)

# Combine results
output_data = [
    {
        "S_SUPPKEY": supplier['S_SUPPKEY'],
        "S_NAME": supplier['S_NAME'],
        "S_ADDRESS": supplier['S_ADDRESS'],
        "S_PHONE": supplier['S_PHONE'],
        "TOTAL_REVENUE": max_total_revenue
    }
    for supplier in supplier_with_max_revenue
]

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "TOTAL_REVENUE"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(output_data)

# Close connections
mysql_cur.close()
mysql_conn.close()
mongo_client.close()
