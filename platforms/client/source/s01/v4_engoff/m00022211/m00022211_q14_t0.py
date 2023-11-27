import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
    # Not using pymysql.cursors.DictCursor as instructed
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Retrieve the promotional parts from MySQL
promotional_parts_query = """
SELECT P_PARTKEY
FROM part
WHERE P_NAME LIKE 'Promo%'
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(promotional_parts_query)
    promotional_parts = [row[0] for row in cursor.fetchall()]

mysql_conn.close()

# Calculate revenue from promotional parts using MongoDB
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)
revenue_aggregation = [
    {
        "$match": {
            "L_PARTKEY": {"$in": promotional_parts},
            "L_SHIPDATE": {
                "$gte": start_date,
                "$lt": end_date
            },
        }
    },
    {
        "$group": {
            "_id": None,
            "promo_revenue": {
                "$sum": {
                    "$multiply": [
                        "$L_EXTENDEDPRICE",
                        {"$subtract": [1, "$L_DISCOUNT"]}
                    ]
                }
            }
        }
    }
]
promo_revenue = list(lineitem_collection.aggregate(revenue_aggregation))

# Calculate total revenue in the date range using MongoDB
total_revenue_aggregation = [
    {
        "$match": {
            "L_SHIPDATE": {
                "$gte": start_date,
                "$lt": end_date
            },
        }
    },
    {
        "$group": {
            "_id": None,
            "total_revenue": {
                "$sum": {
                    "$multiply": [
                        "$L_EXTENDEDPRICE",
                        {"$subtract": [1, "$L_DISCOUNT"]}
                    ]
                }
            }
        }
    }
]
total_revenue = list(lineitem_collection.aggregate(total_revenue_aggregation))

# Calculate percentage
promo_rev = promo_revenue[0]['promo_revenue'] if promo_revenue else 0
total_rev = total_revenue[0]['total_revenue'] if total_revenue else 0
promotion_response_percentage = (promo_rev / total_rev * 100) if total_rev else 0

# Write the output to a file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promotion Effect Percentage'])
    writer.writerow([promotion_response_percentage])
