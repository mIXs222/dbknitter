# promotion_effect_query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to the MySQL database
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
# Create a cursor
cursor = mysql_connection.cursor()
# Query to select relevant data from the lineitem table in the MySQL database
query_mysql = """
SELECT
    L_PARTKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT
FROM
    lineitem
WHERE
    L_SHIPDATE BETWEEN '1995-09-01' AND '1995-10-01';
"""
cursor.execute(query_mysql)
# Fetch the data into pandas DataFrame
lineitem_df = pd.DataFrame(cursor.fetchall(), columns=['L_PARTKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])
# Close the connection to the MySQL database
cursor.close()
mysql_connection.close()

# Connect to the Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)
# Get the 'part' data from Redis and convert it to a pandas DataFrame
part_data = eval(redis_client.get('part'))
part_df = pd.DataFrame(part_data)

# Merging DataFrames on L_PARTKEY to only keep promotional parts, assuming that promotional parts are flagged in the `part` DataFrame
merged_df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the revenue for each line item
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Calculate the total revenue for promotional parts
total_revenue_promo_parts = merged_df['REVENUE'].sum()

# Calculate the total revenue from the lineitem data (as all are shipped within the specified date range)
total_revenue = lineitem_df['L_EXTENDEDPRICE'].sum()

# Calculate the percentage of revenue from promotional parts
percentage_revenue_promo = (total_revenue_promo_parts / total_revenue) * 100

# Output the result to a CSV file
result = pd.DataFrame({'Promotional_Revenue_Percentage': [percentage_revenue_promo]})
result.to_csv('query_output.csv', index=False)
