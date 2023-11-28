# Python code to execute the query (query.py)
import pandas as pd
import pymysql
from datetime import datetime
import direct_redis

# Function to connect to MySQL
def mysql_connection(db_name, user, password, host):
    return pymysql.connect(host=host, user=user, password=password, db=db_name)

# Function to connect to Redis
def redis_connection(host, port, db_num):
    return direct_redis.DirectRedis(host=host, port=port, db=db_num)

# Connect to MySQL
mysql_conn = mysql_connection("tpch", "root", "my-secret-pw", "mysql")

# Query to get line items with shipping date between the given range
lineitem_query = """
SELECT *
FROM lineitem
WHERE L_SHIPDATE >= %s AND L_SHIPDATE <= %s;
"""

# Run the query for line items
with mysql_conn.cursor() as cursor:
    cursor.execute(lineitem_query, ("1995-09-01", "1995-09-30"))
    lineitem_results = cursor.fetchall()

# Convert line items to DataFrame
lineitem_df = pd.DataFrame(lineitem_results, columns=[
    'L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
    'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS',
    'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT',
    'L_SHIPMODE', 'L_COMMENT'])
mysql_conn.close()

# Connect to Redis and get part table
redis_conn = redis_connection("redis", 6379, 0)
part_df_json = redis_conn.get('part')
part_df = pd.read_json(part_df_json, orient='records')

# Ensure L_PARTKEY and P_PARTKEY are of the same type for merging
lineitem_df['L_PARTKEY'] = lineitem_df['L_PARTKEY'].astype(int)
part_df['P_PARTKEY'] = part_df['P_PARTKEY'].astype(int)

# Merge lineitem and part tables on part key
merged_df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filter parts with type starting with 'PROMO'
promo_parts_df = merged_df[merged_df['P_TYPE'].str.startswith('PROMO')]

# Calculate adjusted price (extended price adjusted for discount)
merged_df['ADJUSTED_PRICE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Calculate promotional revenue
promo_revenue = promo_parts_df['ADJUSTED_PRICE'].sum()

# Calculate total revenue
total_revenue = merged_df['ADJUSTED_PRICE'].sum()

# Calculate promotional revenue percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue != 0 else 0

# Prepare the results DataFrame
results_df = pd.DataFrame({
    "Promotional Revenue": [promo_revenue],
    "Total Revenue": [total_revenue],
    "Promotional Revenue Percentage": [promo_revenue_percentage]
})

# Write the query result to a CSV file
results_df.to_csv("query_output.csv", index=False)
