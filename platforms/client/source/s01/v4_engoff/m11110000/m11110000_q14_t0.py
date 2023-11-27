# promotion_effect_query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# Constants for the MySQL Connection
MYSQL_HOST = 'mysql'
MYSQL_DB = 'tpch'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'my-secret-pw'

# Constants for the MongoDB Connection
MONGO_HOST = 'mongodb'
MONGO_PORT = 27017
MONGO_DB = 'tpch'

# Establish connection to MySQL
mysql_conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
mysql_cursor = mysql_conn.cursor()

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
mongo_db = mongo_client[MONGO_DB]

# MongoDB query for fetching promotional parts
promotional_parts = mongo_db.part.find({
    "$expr": {
        "$lte": [{"$dateFromString": {"dateString": "$P_RETAILPRICE.promo_start_date"}},
                 datetime(1995, 9, 1)],
        "$gte": [{"$dateFromString": {"dateString": "$P_RETAILPRICE.promo_end_date"}},
                 datetime(1995, 10, 1)]
    }
})

# Extract part keys of promotional parts
promo_part_keys = [part['P_PARTKEY'] for part in promotional_parts]

# Define the SQL query
sql_query = """
SELECT
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS promo_revenue
FROM
    lineitem
WHERE
    L_SHIPDATE BETWEEN '1995-09-01' AND '1995-10-01'
    AND L_PARTKEY IN (%s)
""" % ','.join(['%s'] * len(promo_part_keys))

# Execute the SQL query
mysql_cursor.execute(sql_query, promo_part_keys)

# Fetch result
promo_revenue = mysql_cursor.fetchone()[0]

# Calculate total revenue in the time range
mysql_cursor.execute("""
SELECT
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
FROM
    lineitem
WHERE
    L_SHIPDATE BETWEEN '1995-09-01' AND '1995-10-01'
""")
total_revenue = mysql_cursor.fetchone()[0]

# Calculate the promotion effect percentage
promo_effect_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Write the output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['promo_effect_percentage'])
    writer.writerow([promo_effect_percentage])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
