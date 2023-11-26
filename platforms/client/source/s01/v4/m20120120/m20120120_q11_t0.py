# python_code.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL and fetch partsupp data
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

mysql_connection = pymysql.connect(**mysql_conn_info)
query_partsupp = "SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST FROM partsupp"
partsupp_df = pd.read_sql(query_partsupp, mysql_connection)
mysql_connection.close()

# Connect to Redis and fetch nation and supplier data
redis_conn_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

r = DirectRedis(**redis_conn_info)
nation_df = r.get('nation')
supplier_df = r.get('supplier')

# Join and perform calculations
# First, filter the nation data for 'Germany'
germany_nations = nation_df[nation_df['N_NAME'] == 'GERMANY']

# Then, join the supplier and nation data frames
supplier_germany = supplier_df[supplier_df['S_NATIONKEY'].isin(germany_nations['N_NATIONKEY'])]

# Now, join the partsupp and supplier data frames
partsupp_supplier_germany = partsupp_df[partsupp_df['PS_SUPPKEY'].isin(supplier_germany['S_SUPPKEY'])]

# Perform the same calculations as in the SQL query
grouped_partsupp = partsupp_supplier_germany.groupby('PS_PARTKEY').apply(lambda x: (x['PS_SUPPLYCOST'] * x['PS_AVAILQTY']).sum()).reset_index(name='VALUE')

# Calculate the threshold value
threshold_value = grouped_partsupp['VALUE'].sum() * 0.0001000000

# Filter the groups according to the HAVING condition
filtered_groups = grouped_partsupp[grouped_partsupp['VALUE'] > threshold_value]

# Sort by VALUE in descending order
sorted_groups = filtered_groups.sort_values(by='VALUE', ascending=False)

# Write output to csv
sorted_groups.to_csv('query_output.csv', index=False)
