import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Fetch part data from MySQL
part_query = """
SELECT P_PARTKEY, P_BRAND, P_CONTAINER
FROM part
WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'
"""
part_df = pd.read_sql(part_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379)

# Fetch lineitem data from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Join datasets
joined_df = part_df.merge(lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Filter based on the subquery
subquery = joined_df.groupby('P_PARTKEY')['L_QUANTITY'].transform('mean') * 0.2
filtered_df = joined_df[joined_df['L_QUANTITY'] < subquery]

# Calculate the result
avg_yearly = (filtered_df['L_EXTENDEDPRICE'].sum() / 7.0)

# Output to CSV
output_df = pd.DataFrame({'AVG_YEARLY': [avg_yearly]})
output_df.to_csv('query_output.csv', index=False)
