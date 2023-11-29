import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Load part supplier from MySQL
query_part_supp = "SELECT PS_PARTKEY, (PS_AVAILQTY * PS_SUPPLYCOST) AS VALUE FROM partsupp"
partsupp_df = pd.read_sql(query_part_supp, mysql_connection)
mysql_connection.close()

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Load nation and supplier data from Redis
nation_df = pd.read_json(redis_connection.get('nation'))
supplier_df = pd.read_json(redis_connection.get('supplier'))

# Filter nations for 'GERMANY' and then join with suppliers
germany_nations = nation_df[nation_df['N_NAME'] == 'GERMANY']
german_suppliers = pd.merge(supplier_df, germany_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Join the german suppliers with partsupp data
relevant_partsupp = pd.merge(partsupp_df, german_suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculate the total value
total_value = relevant_partsupp['VALUE'].sum()

# Find significant parts
significant_parts = relevant_partsupp[relevant_partsupp['VALUE'] > (0.0001 * total_value)]

# Order by value in descending order
significant_parts_sorted = significant_parts.sort_values(by='VALUE', ascending=False)

# Select relevant columns
output = significant_parts_sorted[['PS_PARTKEY', 'VALUE']]

# Write to CSV
output.to_csv('query_output.csv', index=False)
