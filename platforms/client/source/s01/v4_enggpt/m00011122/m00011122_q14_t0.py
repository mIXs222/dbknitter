import pymysql
import pandas as pd

# Define MySQL connection parameters
mysql_conn_info = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch",
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)

# SQL query to get 'part' data from MySQL
mysql_query = """
SELECT P_PARTKEY, P_TYPE
FROM part
WHERE P_TYPE LIKE 'PROMO%%'
"""

# Execute query and fetch 'part' data
part_df = pd.read_sql(mysql_query, con=mysql_conn)
mysql_conn.close()

# Connect to Redis and fetch 'lineitem' data
# Assuming that direct_redis.DirectRedis can read a Pandas DataFrame using get('lineitem')
from direct_redis import DirectRedis

redis_conn = DirectRedis(host="redis", port=6379)
lineitem_df = redis_conn.get('lineitem')
lineitem_df = pd.DataFrame(lineitem_df)

# Convert string dates to datetime objects for comparison
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
start_date = pd.to_datetime('1995-09-01')
end_date = pd.to_datetime('1995-09-30')

# Filter 'lineitem' data for the specific timeframe
lineitem_timeframe_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date)
                                    & (lineitem_df['L_SHIPDATE'] <= end_date)]

# Merge 'part' and 'lineitem' on 'P_PARTKEY' and 'L_PARTKEY'
combined_df = pd.merge(lineitem_timeframe_df, part_df,
                       left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the promotional and total revenues
combined_df['L_DISCOUNTED_PRICE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])
promo_revenue = combined_df[combined_df['P_TYPE'].str.startswith('PROMO')]['L_DISCOUNTED_PRICE'].sum()
total_revenue = combined_df['L_DISCOUNTED_PRICE'].sum()

# Calculate the promotional revenue percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Write the output to a CSV file
output_df = pd.DataFrame({
    'Promotional Revenue': [promo_revenue],
    'Total Revenue': [total_revenue],
    'Promotional Revenue Percentage': [promo_revenue_percentage]
})
output_df.to_csv('query_output.csv', index=False)
