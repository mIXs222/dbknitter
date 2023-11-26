import pymysql
import pandas as pd
import direct_redis

# Connect to the MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# MySQL query for 'partsupp' table
mysql_query = '''
    SELECT
        partsupp.PS_PARTKEY,
        partsupp.PS_SUPPKEY,
        partsupp.PS_AVAILQTY,
        partsupp.PS_SUPPLYCOST
    FROM
        partsupp
'''

# Execute the query on MySQL database
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    partsupp_data = cursor.fetchall()

# Convert the MySQL data to a pandas DataFrame
partsupp_df = pd.DataFrame(partsupp_data, columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST'])

# Close MySQL connection
mysql_conn.close()

# Connect to the Redis database
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'nation' and 'supplier' DataFrames from Redis
nation_df = redis_conn.get('nation')
supplier_df = redis_conn.get('supplier')

# Merge the tables based on the key relationships
merged_df = partsupp_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter for 'GERMANY'
german_supplies_df = merged_df[merged_df['N_NAME'] == 'GERMANY']

# Calculate the total value per PS_PARTKEY
german_supplies_df['VALUE'] = german_supplies_df['PS_SUPPLYCOST'] * german_supplies_df['PS_AVAILQTY']
total_value_per_partkey = german_supplies_df.groupby('PS_PARTKEY')['VALUE'].sum().reset_index()

# Calculate the sum of the value of all German supplies
total_german_value = german_supplies_df['VALUE'].sum() * 0.0001000000

# Filter based on the condition and order by value
final_df = total_value_per_partkey[total_value_per_partkey['VALUE'] > total_german_value].sort_values(by='VALUE', ascending=False)

# Write the final DataFrame to a CSV file
final_df.to_csv('query_output.csv', index=False)
