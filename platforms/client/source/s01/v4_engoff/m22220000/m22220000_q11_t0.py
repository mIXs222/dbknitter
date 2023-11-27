import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to MySQL server
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Execute the query to get partsupp from MySQL
partsupp_query = "SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST FROM partsupp;"
partsupp_df = pd.read_sql(partsupp_query, mysql_conn)
mysql_conn.close()

# Connect to Redis server and get data
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve Redis data as Pandas DataFrames
nation_df = pd.DataFrame(eval(redis_conn.get('nation')))
supplier_df = pd.DataFrame(eval(redis_conn.get('supplier')))

# Filtering suppliers in GERMANY
german_suppliers = nation_df[nation_df['N_NAME'] == 'GERMANY']['N_NATIONKEY'].tolist()
german_supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(german_suppliers)]

# Combining the data
combined_df = pd.merge(
    partsupp_df,
    german_supplier_df,
    left_on='PS_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Calculate the part value
combined_df['PS_VALUE'] = combined_df['PS_AVAILQTY'] * combined_df['PS_SUPPLYCOST']

# Get the total value of all parts to compute the percentage
total_value = combined_df['PS_VALUE'].sum()

# Find parts that represent a significant percentage of the total value
significant_parts_df = combined_df[combined_df['PS_VALUE'] > total_value * 0.0001]

# Select relevant columns and sort by value in descending order
output_df = significant_parts_df[['PS_PARTKEY', 'PS_VALUE']].sort_values('PS_VALUE', ascending=False)

# Write output to CSV
output_df.to_csv('query_output.csv', index=False)
