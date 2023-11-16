import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Get parts that match the condition from MySQL
parts_query = """
SELECT P_PARTKEY
FROM part
WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(parts_query)
    matching_parts = cursor.fetchall()

# Close MySQL connection
mysql_conn.close()

matching_partkeys = tuple([part[0] for part in matching_parts])

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get lineitems from Redis as DataFrame
lineitem_df = pd.DataFrame(eval(redis_conn.get('lineitem')))

# Filter lineitem DataFrame using matching parts
filtered_lineitem_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(matching_partkeys)]

# Calculate the average quantity for the filtered lineitems
avg_quantity = 0.2 * filtered_lineitem_df['L_QUANTITY'].mean()

# Further filter the lineitems where the quantity is less than 0.2 times the average quantity
final_lineitems = filtered_lineitem_df[filtered_lineitem_df['L_QUANTITY'] < avg_quantity]

# Calculate the sum of L_EXTENDEDPRICE divided by 7.0
avg_yearly = final_lineitems['L_EXTENDEDPRICE'].sum() / 7.0

# Write to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['AVG_YEARLY'])
    writer.writerow([avg_yearly])
