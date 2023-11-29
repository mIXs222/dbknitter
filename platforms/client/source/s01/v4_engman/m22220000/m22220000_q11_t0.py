import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Query to select partsupp data from MySQL
ps_query = 'SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST FROM partsupp'
partsupp_df = pd.read_sql(ps_query, mysql_connection)

# Close MySQL connection
mysql_connection.close()

# Connect to Redis database
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from Redis
nation_df = pd.read_json(redis_connection.get('nation'), orient='records')
supplier_df = pd.read_json(redis_connection.get('supplier'), orient='records')

# Filter for suppliers in GERMANY
nation_df = nation_df[nation_df['N_NAME'] == 'GERMANY']
german_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]

# Merge Redis with MySQL data
combined_df = partsupp_df.merge(german_suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculate total value of each part and filter significant parts
combined_df['TOTAL_VALUE'] = combined_df['PS_AVAILQTY'] * combined_df['PS_SUPPLYCOST']
total_value = combined_df['TOTAL_VALUE'].sum()
significant_parts = combined_df[combined_df['TOTAL_VALUE'] > total_value * 0.0001]

# Select part number and value, order by value descending
significant_parts = significant_parts[['PS_PARTKEY', 'TOTAL_VALUE']].sort_values(by='TOTAL_VALUE', ascending=False)

# Write results to CSV file
significant_parts.to_csv('query_output.csv', index=False)

print("Query executed successfully. Check the query_output.csv file for results.")
