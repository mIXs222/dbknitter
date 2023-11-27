import pandas as pd
import pymysql
import direct_redis
import csv

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

try:
    # Define the query for the MySQL database
    mysql_query = """
    SELECT
        ps.PS_PARTKEY,
        COUNT(DISTINCT ps.PS_SUPPKEY) AS supplier_count
    FROM partsupp AS ps
    WHERE ps.PS_PARTKEY NOT IN (
        SELECT p.P_PARTKEY
        FROM part AS p
        WHERE p.P_BRAND = 'Brand#45'
        OR p.P_TYPE LIKE 'MEDIUM POLISHED%'
        OR p.P_SIZE NOT IN (49, 14, 23, 45, 19, 3, 36, 9)
    )
    GROUP BY ps.PS_PARTKEY
    """
    
    # Run the query and get the result for MySQL
    partsupp_df = pd.read_sql(mysql_query, mysql_conn)
finally:
    mysql_conn.close()

# Connect to the Redis database
redis_conn = direct_redis.DirectRedis(host='redis', port=6379)

# Retrieve Redis dataframes
try:
    part_df = pd.read_msgpack(redis_conn.get('part'))
    supplier_df = pd.read_msgpack(redis_conn.get('supplier'))

    # Filter out suppliers with complaints
    supplier_df = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer Complaints')]
    
    # Inner join and filter on known requirements
    filtered_parts = part_df[
        (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])) &
        (~part_df['P_BRAND'].eq('Brand#45')) &
        (~part_df['P_TYPE'].str.contains('MEDIUM POLISHED'))
    ]
    
    partsupp_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(filtered_parts['P_PARTKEY'])]
except Exception as e:
    print(f"An error occurred while retrieving data from Redis: {e}")

# Merge MySQL and Redis dataframes
merged_df = pd.merge(
    partsupp_df, 
    filtered_parts[['P_PARTKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE']], 
    left_on='PS_PARTKEY', 
    right_on='P_PARTKEY'
)

# Sort results as per the specifications
result_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(
    {'supplier_count': 'sum'}
).reset_index().sort_values(
    by=['supplier_count', 'P_BRAND', 'P_TYPE', 'P_SIZE'], 
    ascending=[False, True, True, True]
)

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)
