import csv
import pymysql
import pymongo
from decimal import Decimal
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongodb_client['tpch']

# Run query on MySQL
mysql_query = """
SELECT C_CUSTKEY, C_NAME, C_ADDRESS, N_NAME, C_PHONE, C_ACCTBAL, C_COMMENT
FROM customer
INNER JOIN nation ON C_NATIONKEY = N_NATIONKEY
"""

try:
    mysql_cursor.execute(mysql_query)
    customers = {row[0]: list(row[1:]) for row in mysql_cursor.fetchall()}
finally:
    mysql_cursor.close()
    mysql_conn.close()

# Run aggregation on MongoDB
start_date = datetime(1993, 10, 1)
end_date = datetime(1994, 1, 1)
pipeline = [
    {
        "$match": {
            "L_RETURNFLAG": "R",
            "L_SHIPDATE": {"$gte": start_date, "$lt": end_date}
        }
    },
    {
        "$group": {
            "_id": "$L_ORDERKEY",
            "revenue_lost": {
                "$sum": {
                    "$multiply": [
                        "$L_EXTENDEDPRICE",
                        {"$subtract": [1, "$L_DISCOUNT"]}
                    ]
                }
            }
        }
    },
    {
        "$lookup": {
            "from": "orders",
            "localField": "_id",
            "foreignField": "O_ORDERKEY",
            "as": "order_info"
        }
    },
    {"$unwind": "$order_info"},
    {
        "$group": {
            "_id": "$order_info.O_CUSTKEY",
            "total_revenue_lost": {"$sum": "$revenue_lost"}
        }
    }
]

revenue_lost_per_customer = {doc["_id"]: doc["total_revenue_lost"] for doc in mongodb.lineitem.aggregate(pipeline)}

# Combine results and sort
merged_results = []
for cust_key, details in customers.items():
    if cust_key in revenue_lost_per_customer:
        revenue_lost = revenue_lost_per_customer[cust_key]
        merged_results.append(details + [str(revenue_lost)])

merged_results.sort(key=lambda x: (-Decimal(x[6]), int(cust_key), x[1], Decimal(x[4])))

# Output results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'REVENUE_LOST'])
    for row in merged_results:
        csv_writer.writerow(row)
