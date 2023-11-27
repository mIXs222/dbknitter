import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch",
)
mysql_cursor = mysql_conn.cursor()

# Connect to Redis
redis_conn = DirectRedis(host="redis", port=6379, db=0)

# Query to get the average quantity from lineitem
avg_quantity_query = """
SELECT L_PARTKEY, 0.2 * AVG(L_QUANTITY) AS AVG_QUANTITY
FROM lineitem
GROUP BY L_PARTKEY
"""
mysql_cursor.execute(avg_quantity_query)
avg_quantity = pd.DataFrame(mysql_cursor.fetchall(), columns=['L_PARTKEY', 'AVG_QUANTITY'])

# Fetch part details from Redis
part_details = pd.read_json(redis_conn.get('part'))
filtered_parts = part_details[
    (part_details['P_BRAND'] == 'Brand#23') &
    (part_details['P_CONTAINER'] == 'MED BAG')
]

# Closing the MySQL connection is good practice
mysql_conn.close()

# Read lineitem data from MySQL
query_lineitem = """
SELECT L_PARTKEY, L_QUANTITY, L_EXTENDEDPRICE
FROM lineitem
"""
mysql_cursor.execute(query_lineitem)
lineitem_data = pd.DataFrame(mysql_cursor.fetchall(), columns=['L_PARTKEY', 'L_QUANTITY', 'L_EXTENDEDPRICE'])

# Combine datasets and perform the calculation
combined_data = pd.merge(lineitem_data, avg_quantity, on='L_PARTKEY', how='inner')
combined_data = pd.merge(combined_data, filtered_parts[['P_PARTKEY']], left_on='L_PARTKEY', right_on='P_PARTKEY', how='inner')
filtered_data = combined_data[combined_data['L_QUANTITY'] < combined_data['AVG_QUANTITY']]
result = pd.DataFrame({
    'AVG_YEARLY': [filtered_data['L_EXTENDEDPRICE'].sum() / 7.0]
})

# Write result to CSV file
result.to_csv('query_output.csv', index=False)
