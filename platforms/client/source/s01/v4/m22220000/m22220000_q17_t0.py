import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Define MySQL connection parameters
mysql_connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to the MySQL database
mysql_conn = pymysql.connect(**mysql_connection_params)
mysql_cursor = mysql_conn.cursor()

# MySQL query to get the average quantity from lineitem
avg_quantity_query = """
SELECT L_PARTKEY, 0.2 * AVG(L_QUANTITY) as AVG_QUANTITY
FROM lineitem
GROUP BY L_PARTKEY
"""
mysql_cursor.execute(avg_quantity_query)
avg_quantity_result = mysql_cursor.fetchall()
avg_quantity_df = pd.DataFrame(avg_quantity_result, columns=['L_PARTKEY', 'AVG_QUANTITY'])

# Define Redis connection parameters
redis_connection_params = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connect to the Redis database
redis_conn = DirectRedis(**redis_connection_params)
part_table = pd.read_json(redis_conn.get('part'), orient='records')
# Filtering out records as per condition
part_table_filtered = part_table[(part_table['P_BRAND'] == 'Brand#23') & (part_table['P_CONTAINER'] == 'MED BAG')]

# Merge the MySQL lineitem table with filtered Redis part table on P_PARTKEY and L_PARTKEY
merged_df = pd.merge(left=part_table_filtered, right=avg_quantity_df, left_on='P_PARTKEY', right_on='L_PARTKEY')
 
# MySQL query to get lineitem details
mysql_cursor.execute("SELECT * FROM lineitem")
lineitem_columns = [desc[0] for desc in mysql_cursor.description]
lineitem_result = mysql_cursor.fetchall()
lineitem_df = pd.DataFrame(lineitem_result, columns=lineitem_columns)

# Join merged_df with lineitem_df
final_df = pd.merge(left=merged_df, right=lineitem_df, left_on='L_PARTKEY', right_on='L_PARTKEY')

# Apply the L_QUANTITY condition
final_df = final_df[final_df['L_QUANTITY'] < final_df['AVG_QUANTITY']]

# Calculate AVG_YEARLY
avg_yearly = (final_df['L_EXTENDEDPRICE'].sum() / 7.0)
result_df = pd.DataFrame([avg_yearly], columns=['AVG_YEARLY'])

# Write output to CSV file
result_df.to_csv('query_output.csv', index=False)

# Close the MySQL connection
mysql_cursor.close()
mysql_conn.close()
