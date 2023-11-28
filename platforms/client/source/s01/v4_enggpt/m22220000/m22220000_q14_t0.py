import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Query to select data from lineitem table for the specified date range
query_mysql = """
SELECT 
    L_ORDERKEY, L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
FROM 
    lineitem 
WHERE 
    L_SHIPDATE BETWEEN '1995-09-01' AND '1995-09-30';
"""

# Read query results into a DataFrame
lineitem_df = pd.read_sql(query_mysql, mysql_conn)

# Close the MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get 'part' DataFrame from Redis
part_df = pd.read_json(redis_conn.get('part'), orient='records')

# Filter parts where type starts with 'PROMO'
promo_part_df = part_df[part_df['P_TYPE'].str.startswith('PROMO')]

# Merge lineitem and part data on part key and filter promo parts
merged_df = pd.merge(lineitem_df, promo_part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate adjusted extended price for promo parts
merged_df['ADJUSTED_PRICE_PROMO'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Calculate total promotional revenue
promo_revenue = merged_df['ADJUSTED_PRICE_PROMO'].sum()

# Calculate adjusted extended prices for all line items
lineitem_df['ADJUSTED_PRICE_TOTAL'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Calculate total revenue
total_revenue = lineitem_df['ADJUSTED_PRICE_TOTAL'].sum()

# Calculate promotional revenue percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Write the result to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PROMO_REVENUE_PERCENTAGE'])
    writer.writerow([promo_revenue_percentage])

print(f"Promotional revenue as a percentage of total revenue: {promo_revenue_percentage}%")
