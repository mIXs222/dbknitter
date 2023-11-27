import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_connection.cursor()

# Execute query on MySQL to fetch lineitem data
mysql_query = """
SELECT L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPMODE, L_SHIPINSTRUCT
FROM lineitem
WHERE (L_SHIPMODE IN ('AIR', 'AIR REG')
       AND L_SHIPINSTRUCT = 'DELIVER IN PERSON')
"""
mysql_cursor.execute(mysql_query)
lineitem_data = mysql_cursor.fetchall()

# Convert lineitem data to DataFrame
lineitem_df = pd.DataFrame(lineitem_data, columns=['L_ORDERKEY', 'L_PARTKEY', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPMODE', 'L_SHIPINSTRUCT'])

# Close the MySQL connection
mysql_cursor.close()
mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get part data from Redis and convert to DataFrame
part_data_json = redis_connection.get('part')
part_df = pd.read_json(part_data_json)

# Merge DataFrames (similar to SQL JOIN)
merged_df = lineitem_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Apply the conditions specified in the SQL query
conditions = (
    ((merged_df['P_BRAND'] == 'Brand#12') & (merged_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & 
     (merged_df['L_QUANTITY'] >= 1) & (merged_df['L_QUANTITY'] <= 11) & 
     (merged_df['P_SIZE'] >= 1) & (merged_df['P_SIZE'] <= 5))
    |
    ((merged_df['P_BRAND'] == 'Brand#23') & (merged_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & 
     (merged_df['L_QUANTITY'] >= 10) & (merged_df['L_QUANTITY'] <= 20) & 
     (merged_df['P_SIZE'] >= 1) & (merged_df['P_SIZE'] <= 10))
    |
    ((merged_df['P_BRAND'] == 'Brand#34') & (merged_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & 
     (merged_df['L_QUANTITY'] >= 20) & (merged_df['L_QUANTITY'] <= 30) & 
     (merged_df['P_SIZE'] >= 1) & (merged_df['P_SIZE'] <= 15))
)

filtered_df = merged_df[conditions]

# Calculate revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by order key and calculate sum of revenue
result_df = filtered_df.groupby('L_ORDERKEY').agg({'REVENUE': 'sum'}).reset_index()

# Write the results to a CSV file
result_df.to_csv('query_output.csv', index=False)
