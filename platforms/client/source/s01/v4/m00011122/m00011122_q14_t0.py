# hybrid_query.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Fetch part data from MySQL
part_query = "SELECT P_PARTKEY, P_TYPE FROM part"
part_df = pd.read_sql(part_query, mysql_conn)

# Close the MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch lineitem data from Redis and convert to Pandas DataFrame
lineitem_df = pd.read_msgpack(redis_conn.get('lineitem'))

# Filter lineitem based on date
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1995-09-01') & (lineitem_df['L_SHIPDATE'] < '1995-10-01')]

# Merge DataFrames on L_PARTKEY
merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate PROMO_REVENUE
promo_revenue = (
    100.00 * 
    merged_df[merged_df['P_TYPE'].str.startswith('PROMO')]['L_EXTENDEDPRICE'] * 
    (1 - merged_df[merged_df['P_TYPE'].str.startswith('PROMO')]['L_DISCOUNT'])
).sum() / (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])).sum()

# Create result DataFrame
result_df = pd.DataFrame([{'PROMO_REVENUE': promo_revenue}])

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
