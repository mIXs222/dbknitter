import pandas as pd
import pymysql
import csv
from direct_redis import DirectRedis

# Connect to the MySQL database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Connect to the Redis database
redis_client = DirectRedis(host='redis', port=6379)

# Read in supplier and nation tables from MySQL
with mysql_connection.cursor() as cursor:
    cursor.execute("SELECT * FROM supplier")
    supplier_data = cursor.fetchall()
    cursor.execute("SELECT * FROM nation")
    nation_data = cursor.fetchall()

# Columns for supplier and nation tables
supplier_columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
nation_columns = ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT']

# Convert to DataFrames
df_supplier = pd.DataFrame(list(supplier_data), columns=supplier_columns)
df_nation = pd.DataFrame(list(nation_data), columns=nation_columns)

# Filter for nation = GERMANY
df_nation_germany = df_nation[df_nation['N_NAME'] == 'GERMANY']

# Merge on S_NATIONKEY
df_supplier_germany = pd.merge(df_supplier, df_nation_germany, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Retrieve partsupp DataFrame from Redis
df_partsupp = pd.read_json(redis_client.get('partsupp'), orient='records')

# Execute the join using pandas (equivalent to the SQL WHERE clause)
df_combined = pd.merge(df_partsupp, df_supplier_germany, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Perform the GROUP BY and HAVING operations
grouped = df_combined.groupby('PS_PARTKEY')
df_result = grouped['PS_SUPPLYCOST', 'PS_AVAILQTY'].apply(
    lambda x: sum(x['PS_SUPPLYCOST'] * x['PS_AVAILQTY'])
).reset_index(name='VALUE')

# Perform the subquery
total_value = df_combined['PS_SUPPLYCOST'].multiply(df_combined['PS_AVAILQTY']).sum() * 0.0001000000

# Filter according to HAVING clause
df_result = df_result[df_result['VALUE'] > total_value]

# Sort the DataFrame
df_result.sort_values(by='VALUE', ascending=False, inplace=True)

# Write the result to a CSV file
df_result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close the MySQL connection
mysql_connection.close()
