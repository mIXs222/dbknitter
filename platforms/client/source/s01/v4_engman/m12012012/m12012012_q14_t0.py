import pymysql
import pandas as pd
import direct_redis
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Redis connection setup
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve promotion parts from MySQL
mysql_query = """
SELECT P_PARTKEY, P_RETAILPRICE
FROM part
WHERE P_RETAILPRICE > 0 AND P_NAME LIKE 'PROMO%'
"""
mysql_cursor.execute(mysql_query)
promotion_parts = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_RETAILPRICE'])

# Retrieve lineitem data from Redis within the specified date range
start_date = '1995-09-01'
end_date = '1995-10-01'
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Filter line items with dates within the desired range.
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)]

# Merge two DataFrames on part key
merged_df = pd.merge(filtered_lineitem_df, promotion_parts, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate revenue for promotion parts and total revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
total_promo_revenue = sum(merged_df['REVENUE'])
total_revenue = sum(filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT']))

# Calculate and store the percentage of promo revenue
percentage_promo_revenue = 0 if total_revenue == 0 else (total_promo_revenue / total_revenue) * 100
result = [{'percentage_promo_revenue': percentage_promo_revenue}]

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=result[0].keys())
    writer.writeheader()
    writer.writerows(result)

# Close MySQL cursor and connection
mysql_cursor.close()
mysql_conn.close()
