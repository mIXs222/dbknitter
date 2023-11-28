import pymysql
import pandas as pd
import direct_redis
from datetime import datetime

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Querying part table
part_query = """
SELECT P_PARTKEY, P_TYPE
FROM part
WHERE P_TYPE LIKE 'PROMO%%';
"""
part_df = pd.read_sql(part_query, mysql_conn)
mysql_conn.close()

# Establishing connection to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read lineitem DataFrame from Redis
lineitem_df = redis_conn.get('lineitem')

# Convert shipping date from string to datetime and filter by date range
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'], format='%Y-%m-%d')
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 9, 30)
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) & 
    (lineitem_df['L_SHIPDATE'] <= end_date)
]

# Merge lineitem with part on part key
merged_df = pd.merge(filtered_lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate Discounted Extended Price
merged_df['DISCOUNTED_PRICE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Calculate Promotional Revenue
promo_revenue = merged_df[merged_df['P_TYPE'].str.startswith('PROMO')]['DISCOUNTED_PRICE'].sum()

# Calculate Total Revenue
total_revenue = merged_df['DISCOUNTED_PRICE'].sum()

# Calculate Promotional Revenue Percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0.0

# Save Result To a CSV File
result_df = pd.DataFrame({
    'Promo_Revenue_Percentage': [promo_revenue_percentage]
})
result_df.to_csv('query_output.csv', index=False)
