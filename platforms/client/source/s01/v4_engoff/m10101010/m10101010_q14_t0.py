# promotion_effect_query.py

import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# MongoDB setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Get promotional parts from MongoDB
promotional_parts_cursor = mongodb['part'].find({})
promotional_parts = set()
for part in promotional_parts_cursor:
    promotional_parts.add(part['P_PARTKEY'])

# Query MySQL
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        revenue_query = """
            SELECT
                L_PARTKEY,
                SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
            FROM
                lineitem
            WHERE
                L_SHIPDATE >= '1995-09-01'
                AND L_SHIPDATE < '1995-10-01'
                AND L_PARTKEY IN ({})

            GROUP BY L_PARTKEY;
        """.format(','.join(str(part) for part in promotional_parts))
        cursor.execute(revenue_query)
        result = cursor.fetchall()

# Calculate promotional revenue and total revenue
promotional_revenue = sum(revenue for _, revenue in result)
total_revenue_query = """
    SELECT
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
    FROM
        lineitem
    WHERE
        L_SHIPDATE >= '1995-09-01'
        AND L_SHIPDATE < '1995-10-01';
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(total_revenue_query)
    total_revenue = cursor.fetchone()[0]

# Calculate the promotion effect
promotional_effect = (promotional_revenue / total_revenue) * 100 if total_revenue else 0

# Write query output to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promotion Effect Percentage'])
    writer.writerow([f"{promotional_effect:.2f}"])

# Close connections
mysql_conn.close()
mongo_client.close()
