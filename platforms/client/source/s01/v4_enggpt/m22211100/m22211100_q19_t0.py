import pymysql
import pandas as pd

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Execute MySQL Query
mysql_query = """
SELECT 
    L_ORDERKEY, 
    L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS REVENUE,
    L_PARTKEY,
    L_QUANTITY,
    L_SHIPMODE,
    L_SHIPINSTRUCT
FROM 
    lineitem
"""
lineitem_df = pd.read_sql(mysql_query, con=mysql_connection)
mysql_connection.close()

# Connect to Redis
import direct_redis

redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
part_df = pd.DataFrame(eval(redis_connection.get('part')))

# Combine DataFrames
combined_df = pd.merge(
    lineitem_df,
    part_df,
    how='inner',
    left_on='L_PARTKEY',
    right_on='P_PARTKEY'
)

# Define the filtering conditions
conditions = [
    (combined_df['P_BRAND'] == 'Brand#12')
    & (combined_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']))
    & (combined_df['L_QUANTITY'] >= 1) & (combined_df['L_QUANTITY'] <= 11)
    & (combined_df['P_SIZE'] >= 1) & (combined_df['P_SIZE'] <= 5)
    & (combined_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']))
    & (combined_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),

    (combined_df['P_BRAND'] == 'Brand#23')
    & (combined_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']))
    & (combined_df['L_QUANTITY'] >= 10) & (combined_df['L_QUANTITY'] <= 20)
    & (combined_df['P_SIZE'] >= 1) & (combined_df['P_SIZE'] <= 10)
    & (combined_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']))
    & (combined_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),

    (combined_df['P_BRAND'] == 'Brand#34')
    & (combined_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']))
    & (combined_df['L_QUANTITY'] >= 20) & (combined_df['L_QUANTITY'] <= 30)
    & (combined_df['P_SIZE'] >= 1) & (combined_df['P_SIZE'] <= 15)
    & (combined_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']))
    & (combined_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
]

# Apply filtering conditions
filtered_df = combined_df[
    conditions[0] | conditions[1] | conditions[2]
]

# Calculate total revenue
revenue = filtered_df['REVENUE'].sum()

# Write results to file
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
filtered_df.to_csv('query_output.csv', index=False)
