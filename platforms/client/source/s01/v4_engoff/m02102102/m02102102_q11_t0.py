# import necessary libraries
import pandas as pd
import pymysql
import direct_redis

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Execute the necessary queries in MySQL
with mysql_conn.cursor() as cursor:
    
    # Query to get nations in GERMANY and their nation keys
    cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY'")
    german_nation_keys = cursor.fetchall()
    german_nation_keys = [key[0] for key in german_nation_keys]
    
    # Query to get suppliers from GERMANY
    format_strings = ','.join(['%s'] * len(german_nation_keys))
    cursor.execute("SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY IN (%s)" % format_strings,
                   tuple(german_nation_keys))
    german_supplier_keys = cursor.fetchall()
    german_supplier_keys = [key[0] for key in german_supplier_keys]
    
mysql_conn.close()

# Connect to Redis
rds = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get partsupp table and convert to Pandas DataFrame
partsupp_data = rds.get('partsupp')
partsupp_df = pd.read_json(partsupp_data)

# Filter for only parts supplied by German suppliers
filtered_partsupp_df = partsupp_df[partsupp_df['PS_SUPPKEY'].isin(german_supplier_keys)]

# Calculate total value for all parts
filtered_partsupp_df['TOTAL_VALUE'] = filtered_partsupp_df['PS_AVAILQTY'] * filtered_partsupp_df['PS_SUPPLYCOST']

# Find the total value in the dataframe
total_value = filtered_partsupp_df['TOTAL_VALUE'].sum()

# Get parts and their values that represent a significant percentage of the total value
important_parts = filtered_partsupp_df[filtered_partsupp_df['TOTAL_VALUE'] / total_value > 0.0001]

# Select relevant columns and sort by value
important_parts = important_parts[['PS_PARTKEY', 'TOTAL_VALUE']].sort_values(by='TOTAL_VALUE', ascending=False)

# Write to CSV
important_parts.to_csv('query_output.csv', index=False)
