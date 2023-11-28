import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query for lineitem data from MySQL
lineitem_query = """
SELECT L_PARTKEY, L_EXTENDEDPRICE, L_QUANTITY
FROM lineitem
"""
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get part data from Redis and convert it to a DataFrame
part_data = redis_conn.get('part')
part_df = pd.read_json(part_data)

# Filter parts with 'Brand#23' and 'MED BAG'
filtered_parts_df = part_df[(part_df['P_BRAND'] == 'Brand#23') &
                            (part_df['P_CONTAINER'] == 'MED BAG')]

# Calculate the average quantity for each P_PARTKEY
avg_quantities = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_quantities['avg_20pct'] = avg_quantities['L_QUANTITY'] * 0.20

# Merge to get average quantities for each part
lineitem_df = pd.merge(lineitem_df, avg_quantities, on='L_PARTKEY')

# Filter line items where quantity is less than 20% of the average quantity
lineitem_filtered_df = lineitem_df[lineitem_df['L_QUANTITY'] < lineitem_df['avg_20pct']]

# Merge line items with the filtered parts
merge_df = pd.merge(lineitem_filtered_df, filtered_parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the average yearly extended price
merge_df['avg_yearly_price'] = merge_df['L_EXTENDEDPRICE'] / 7.0

# Group by P_PARTKEY and sum the average yearly prices
result_df = merge_df.groupby('P_PARTKEY')['avg_yearly_price'].sum().reset_index()

# Write the result to a csv file
result_df.to_csv('query_output.csv', index=False)
