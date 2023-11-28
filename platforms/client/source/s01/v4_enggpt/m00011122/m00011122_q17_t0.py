import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Establish connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = mysql_conn.cursor()

# Fetch parts from MySQL with the specified brand and container type
part_query = """
SELECT P_PARTKEY, P_NAME
FROM part
WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'
"""
cursor.execute(part_query)
part_result = cursor.fetchall()
part_df = pd.DataFrame(part_result, columns=['P_PARTKEY', 'P_NAME'])
part_keys = part_df['P_PARTKEY'].tolist()

# Establish connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem dataframe from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Calculate the average quantity for each part across all lineitems
avg_quantity = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_quantity = avg_quantity.rename(columns={'L_QUANTITY': 'AVG_QUANTITY'})

# Merge to get the average quantity in lineitem dataframe
lineitem_df = pd.merge(lineitem_df, avg_quantity, on='L_PARTKEY', how='left')

# Filter lineitem with quantity less than 20% of the average quantity of the same part
lineitem_df = lineitem_df[lineitem_df['L_QUANTITY'] < 0.2 * lineitem_df['AVG_QUANTITY']]

# Combine filtered lineitem with the part dataframe based on the part keys
result_df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the average yearly extended price
result_df['AVG_YEARLY_EXTENDED_PRICE'] = result_df['L_EXTENDEDPRICE'].sum() / 7.0

# Select the required columns for final output
output_df = result_df[['P_PARTKEY', 'P_NAME', 'AVG_YEARLY_EXTENDED_PRICE']]

# Save results to csv
output_df.to_csv('query_output.csv', index=False)

# Close database connections
cursor.close()
mysql_conn.close()
redis_conn.close()
