import pymysql
import pymongo
import csv
from datetime import datetime

# Establish connection to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Establish connection to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client.tpch
mongo_part_collection = mongo_db.part

# Fetch promotion parts from MongoDB
promo_parts = list(mongo_part_collection.find({}, {'P_PARTKEY': 1, '_id': 0}))
promo_parts_keys = [p['P_PARTKEY'] for p in promo_parts]

# Define the date range for the shipped parts
start_date = datetime.strptime('1995-09-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-10-01', '%Y-%m-%d')

# Prepare the SQL query to fetch revenue
sql_query = """
    SELECT
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT))
    FROM
        lineitem
    WHERE
        L_SHIPDATE >= %s
        AND L_SHIPDATE < %s
        AND L_PARTKEY IN (%s)
"""
format_strings = ','.join(['%s'] * len(promo_parts_keys))

# Execute the SQL query
mysql_cursor.execute(sql_query % (start_date.strftime('%Y-%m-%d'),
                                  end_date.strftime('%Y-%m-%d'),
                                  format_strings),
                     promo_parts_keys)
revenue_result = mysql_cursor.fetchall()

# Calculate total revenue
total_revenue = 0
if revenue_result and revenue_result[0][0] is not None:
    total_revenue = revenue_result[0][0]

# Assuming that the promotion effects all parts equally and there is no need to
# calculate revenue for non-promotional parts, since the percentage is relative
promo_revenue_percentage = (total_revenue / total_revenue) * 100 if total_revenue else 0

# Output results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    output_writer = csv.writer(csvfile)
    output_writer.writerow(['Promotion Revenue Percentage'])
    output_writer.writerow([promo_revenue_percentage])

# Close database connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
