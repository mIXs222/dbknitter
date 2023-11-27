# suppliers_who_kept_orders_waiting.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to the MySQL database
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to Redis
redis_conn_info = {
    'host': 'redis',
    'port': 6379,
}

# Connect to MySQL
mysql_db = pymysql.connect(**mysql_conn_info)

# Connect to Redis
redis_db = DirectRedis(**redis_conn_info)

# Query to get lineitem data from mysql
lineitem_query = """
SELECT L_ORDERKEY, L_SUPPKEY, L_COMMITDATE, L_RECEIPTDATE
FROM lineitem
WHERE L_LINESTATUS = 'F'
"""

# Execute the query and fetch the result
lineitem_df = pd.read_sql(lineitem_query, mysql_db)

# Get data from Redis
nation_df = pd.read_json(redis_db.get('nation'), orient='records', lines=True)
supplier_df = pd.read_json(redis_db.get('supplier'), orient='records', lines=True)
orders_df = pd.read_json(redis_db.get('orders'), orient='records', lines=True)

# Close the MySQL connection
mysql_db.close()

# Merge the dataframes to filter the relevant data
df = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df = df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df = df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter for SAUDI ARABIA and failed to meet the committed delivery date
df_result = df[(df['N_NAME'] == 'SAUDI ARABIA') & (df['L_COMMITDATE'] < df['L_RECEIPTDATE'])]

# Select only unique suppliers
df_result = df_result[['S_SUPPKEY', 'S_NAME']].drop_duplicates()

# Write the query output to CSV file
df_result.to_csv('query_output.csv', index=False)
