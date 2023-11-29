import pymysql
import pymongo
import csv
from datetime import datetime


# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4'
)
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
mongodb_part_col = mongodb_db['part']

# Find promotional parts in MongoDB
promotion_parts = mongodb_part_col.find(
    {'$or': [{'P_BRAND': 'Brand#45'},
             {'P_TYPE': {'$regex': 'PROMO'}}]}
)
promo_part_keys = {part['P_PARTKEY'] for part in promotion_parts}

# Execute MySQL query on lineitem
query = """
SELECT
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS promo_revenue
FROM
    lineitem
WHERE
    L_SHIPDATE BETWEEN '1995-09-01' AND '1995-10-01'
    AND L_PARTKEY IN (%s)
"""
# Format the IN clause with placeholders
in_p = ', '.join(['%s'] * len(promo_part_keys))
query = query % in_p

# Run the MySQL query with the list of promo part keys
mysql_cursor.execute(query, list(promo_part_keys))
result = mysql_cursor.fetchone()
promo_revenue = result[0] if result else 0

# Get total revenue in the period
query_total_revenue = """
SELECT
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
FROM
    lineitem
WHERE
    L_SHIPDATE BETWEEN '1995-09-01' AND '1995-10-01'
"""
mysql_cursor.execute(query_total_revenue)
result = mysql_cursor.fetchone()
total_revenue = result[0] if result else 0

# Calculate the promotion effect
promo_effect = (promo_revenue / total_revenue * 100) if total_revenue else 0

# Close the cursor and MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Write result to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Promotion Effect'])
    writer.writerow([f'{promo_effect:.2f}'])

print('Query execution finished. The output is saved in query_output.csv.')
