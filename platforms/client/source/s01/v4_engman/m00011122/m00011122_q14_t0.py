import pymysql
import pandas as pd
import direct_redis

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Query to select parts from the MySQL database
mysql_query = """
SELECT P_PARTKEY, P_RETAILPRICE
FROM part
WHERE P_NAME LIKE '%Promo%'
"""

# Execute query on MySQL
mysql_parts = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connection setup for Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
redis_lineitem_df = redis_conn.get('lineitem')

# If the data returned is not a DataFrame, let's convert it to DataFrame
if not isinstance(redis_lineitem_df, pd.DataFrame):
    raise ValueError("Data from Redis is not a pandas DataFrame")

# Filter for the dates and calculate revenue
filtered_lineitem = redis_lineitem_df[(
    (redis_lineitem_df['L_SHIPDATE'] >= '1995-09-01') &
    (redis_lineitem_df['L_SHIPDATE'] <= '1995-10-01'))
]

# Merge the dataframes on partkey to associate parts with lineitems
merged_df = pd.merge(
    filtered_lineitem,
    mysql_parts,
    left_on='L_PARTKEY',
    right_on='P_PARTKEY',
    how='inner'
)

# Calculate the revenue for each lineitem
merged_df['revenue'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Aggregate the total revenue from promotional parts
total_revenue = merged_df['revenue'].sum()

# Output the results to a CSV file
merged_df.to_csv('query_output.csv', index=False)

# Calculate percentage revenue from promotional parts
if total_revenue > 0:
    promo_revenue = merged_df.loc[merged_df['P_NAME'].str.contains('Promo'), 'revenue'].sum()
    promo_percent = (promo_revenue / total_revenue) * 100
    with open('query_output.csv', 'a') as f:
        f.write(f"\nPromotional Revenue Percentage: {promo_percent:.2f}%")
else:
    with open('query_output.csv', 'a') as f:
        f.write("\nPromotional Revenue Percentage: No revenue in specified period.")
