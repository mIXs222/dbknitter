import csv
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')
cursor = mysql_conn.cursor()

# Retrieve lineitem data from MySQL that matches the condition
lineitem_query = """
SELECT L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT
FROM lineitem
WHERE L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE < '1995-10-01'
"""
cursor.execute(lineitem_query)
lineitem_data = cursor.fetchall()

# Close MySQL connection
cursor.close()
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve part data from Redis
part_data = pd.DataFrame(eval(redis_conn.get('part')))

# Filter part data with condition
part_data_filtered = part_data[part_data['P_TYPE'].str.startswith('PROMO')]

# Create a DataFrame from the lineitem data
lineitem_df = pd.DataFrame(list(lineitem_data), columns=['L_PARTKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])

# Merge lineitem and part data on P_PARTKEY
merged_data = pd.merge(lineitem_df, part_data_filtered, left_on='L_PARTKEY', right_on='P_PARTKEY', how='inner')

# Calculate PROMO_REVENUE
merged_data['DISCOUNT_PRICE'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])
promo_revenue = (merged_data['DISCOUNT_PRICE'].sum() / lineitem_df['DISCOUNT_PRICE'].sum()) * 100

# Write result to file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PROMO_REVENUE'])
    writer.writerow([promo_revenue])

print("Query results have been saved to 'query_output.csv'")
