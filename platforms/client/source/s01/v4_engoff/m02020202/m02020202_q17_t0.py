import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    passwd="my-secret-pw",
    db="tpch"
)

# Retrieve parts with brand 23 and container 'MED BAG' from MySQL
part_query = """
    SELECT P_PARTKEY FROM part
    WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG';
"""
parts_df = pd.read_sql(part_query, mysql_conn)
mysql_conn.close()

# Setup connection with Redis
redis_conn = direct_redis.DirectRedis(host="redis", port=6379)

# Extract lineitem data from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Merge datasets
merged_df = pd.merge(parts_df, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Calculate average quantity for parts
average_quantity = merged_df['L_QUANTITY'].mean()

# Calculate potential lost revenue
potential_lost_revenue_df = merged_df[merged_df['L_QUANTITY'] < 0.2 * average_quantity]

# Determine average yearly gross loss in revenue
potential_lost_revenue_df['YEAR'] = pd.to_datetime(potential_lost_revenue_df['L_SHIPDATE']).dt.year
yearly_lost_revenue = potential_lost_revenue_df.groupby('YEAR')['L_EXTENDEDPRICE'].sum()
average_yearly_lost_revenue = yearly_lost_revenue.mean()

# Output result to a CSV file
result_df = pd.DataFrame({'Average Yearly Lost Revenue': [average_yearly_lost_revenue]})
result_df.to_csv('query_output.csv', index=False)
