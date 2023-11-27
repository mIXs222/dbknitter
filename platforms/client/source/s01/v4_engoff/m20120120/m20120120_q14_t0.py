# query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB Connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Check for promotional parts in MongoDB
promotional_parts = mongodb_db['part'].find({},
                                            {'P_PARTKEY': 1, '_id': 0})
promotional_partkeys = [pp['P_PARTKEY'] for pp in promotional_parts]

# Fetch lineitem data from MySQL and calculate revenue
with mysql_conn.cursor() as cursor:
    query = """
    SELECT L_PARTKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
    FROM lineitem
    WHERE L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE < '1995-10-01' 
          AND L_PARTKEY IN ({})
    GROUP BY L_PARTKEY
    """.format(', '.join(['%s'] * len(promotional_partkeys)))
    
    cursor.execute(query, promotional_partkeys)
    result = cursor.fetchall()

# Calculating total revenue
total_revenue = sum(row[1] for row in result)

# Calculating promotional revenue
promotional_revenue = sum(row[1] for row in result if row[0] in promotional_partkeys)

# Calculating percentage
percentage = (promotional_revenue / total_revenue) * 100 if total_revenue else 0

# Write output to csv file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promotional Revenue Percentage'])
    writer.writerow([percentage])

print("Query completed. Output written to query_output.csv.")

# Close connections
mysql_conn.close()
mongodb_client.close()
