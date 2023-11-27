# import_dependencies.py
import csv
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Fetch data from the nation table in MySQL
with mysql_connection.cursor() as cursor:
    query = "SELECT * FROM nation WHERE N_NAME = 'GERMANY';"
    cursor.execute(query)
    germany_nations = cursor.fetchall()

# Convert fetched data to pandas dataframe
germany_nations_df = pd.DataFrame(germany_nations, columns=["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"])

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from the supplier table in Redis
supplier_df = pd.read_json(redis_connection.get('supplier'))

# Fetch data from the partsupp table in Redis
partsupp_df = pd.read_json(redis_connection.get('partsupp'))

# Filter suppliers from GERMANY
germany_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(germany_nations_df['N_NATIONKEY'])]

# Merge suppliers with their parts
merged_data = pd.merge(germany_suppliers, partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Calculate the value of available parts (PS_AVAILQTY * PS_SUPPLYCOST)
merged_data['PART_VALUE'] = merged_data['PS_AVAILQTY'] * merged_data['PS_SUPPLYCOST']

# Sum the total value of all parts to find significance threshold
total_value = merged_data['PART_VALUE'].sum()
significance_threshold = total_value * 0.0001

# Find all parts above the significance threshold
significant_parts = merged_data[merged_data['PART_VALUE'] > significance_threshold]

# Select relevant columns and sort by PART_VALUE
result = significant_parts[['PS_PARTKEY', 'PART_VALUE']].sort_values(by='PART_VALUE', ascending=False)

# Write to query_output.csv
result.to_csv('query_output.csv', index=False)

# Clean up connections
mysql_connection.close()
