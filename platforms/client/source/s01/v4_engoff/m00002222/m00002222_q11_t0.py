# query_code.py

import pymysql
import pandas as pd
import direct_redis

# Constants to connect to MySQL
MYSQL_DB_NAME = "tpch"
MYSQL_USER = "root"
MYSQL_PASSWORD = "my-secret-pw"
MYSQL_HOST = "mysql"

# Connect to MySQL
mysql_connection = pymysql.connect(host=MYSQL_HOST,
                                   user=MYSQL_USER,
                                   password=MYSQL_PASSWORD,
                                   database=MYSQL_DB_NAME)
mysql_cursor = mysql_connection.cursor()

# Get the suppliers and nation tables from MySQL
supplier_query = "SELECT S_SUPPKEY, S_NAME, S_NATIONKEY from supplier WHERE S_NATIONKEY = (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY')"
mysql_cursor.execute(supplier_query)
supplier_results = mysql_cursor.fetchall()

# Convert to DataFrame
supplier_df = pd.DataFrame(supplier_results, columns=['S_SUPPKEY', 'S_NAME', 'S_NATIONKEY'])

# Constants to connect to Redis
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 0
redis_connection = direct_redis.DirectRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# Get the partsupp table from Redis
partsupp_df = pd.read_json(redis_connection.get('partsupp'))

# Merge supplier and partsupp dataframes on S_SUPPKEY
merged_df = pd.merge(supplier_df, partsupp_df, how='inner', left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Calculate total value for each part as PS_AVAILQTY * PS_SUPPLYCOST
merged_df['TOTAL_VALUE'] = merged_df['PS_AVAILQTY'] * merged_df['PS_SUPPLYCOST']

# Find sum of total values to calculate percentage
sum_total_value = merged_df['TOTAL_VALUE'].sum()

# Filter rows where the total value of the part is greater than 0.0001 of the sum of all total values
important_stock_df = merged_df[merged_df['TOTAL_VALUE'] > sum_total_value * 0.0001]

# Selecting part number and value columns
output_df = important_stock_df[['PS_PARTKEY', 'TOTAL_VALUE']].sort_values(by='TOTAL_VALUE', ascending=False)

# Write the results to a CSV file
output_df.to_csv('query_output.csv', index=False)

# Close MySQL cursor and connection
mysql_cursor.close()
mysql_connection.close()
