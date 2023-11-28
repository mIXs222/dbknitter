import pymysql
import pymongo
import datetime
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
part_collection = mongodb['part']

# Prepare queries and parameters
start_date = datetime.datetime(1995, 9, 1)
end_date = datetime.datetime(1995, 9, 30)

# Fetch parts that are promotional
promo_parts = list(
    part_collection.find(
        {'P_TYPE': {'$regex': '^PROMO'}},
        {'P_PARTKEY': 1, '_id': 0}
    )
)

promo_part_keys = [part['P_PARTKEY'] for part in promo_parts]

# Query MySQL
cur = mysql_connection.cursor()

# For the sum of promotional revenues
promo_sum_query = """
    SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS promo_revenue
    FROM lineitem
    WHERE L_SHIPDATE BETWEEN %s AND %s
    AND L_PARTKEY IN %s
"""
cur.execute(promo_sum_query, (start_date, end_date, tuple(promo_part_keys)))
promo_revenue = cur.fetchone()[0] if cur.rowcount else 0

# For the sum of total revenues
total_sum_query = """
    SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
    FROM lineitem
    WHERE L_SHIPDATE BETWEEN %s AND %s
"""
cur.execute(total_sum_query, (start_date, end_date))
total_revenue = cur.fetchone()[0] if cur.rowcount else 0

# Calculate promotional revenue percentage
promo_percentage = (promo_revenue / total_revenue * 100) if total_revenue else 0

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Promo Revenue Percentage'])
    writer.writerow([promo_percentage])

# Close connections
cur.close()
mysql_connection.close()
mongo_client.close()
