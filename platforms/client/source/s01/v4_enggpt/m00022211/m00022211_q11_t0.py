import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MySQL and retrieve 'nation' table
with mysql_connection.cursor() as cursor:
    query_nation = "SELECT * FROM nation WHERE N_NAME = 'GERMANY';"
    cursor.execute(query_nation)
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
    germany_nationkey = nations['N_NATIONKEY'].iloc[0]

# Close MySQL connection
mysql_connection.close()

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'supplier' and 'partsupp' tables from Redis
supplier_df = pd.read_json(redis_client.get('supplier'))
partsupp_df = pd.read_json(redis_client.get('partsupp'))

# Apply filter on suppliers being from Germany using the nation key
german_suppliers = supplier_df[supplier_df['S_NATIONKEY'] == germany_nationkey]

# Merge partsupp and german suppliers
merged_df = pd.merge(
    partsupp_df,
    german_suppliers,
    how='inner',
    left_on='PS_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Calculate value for each part and supplier
merged_df['VALUE'] = merged_df['PS_SUPPLYCOST'] * merged_df['PS_AVAILQTY']

# Group by part key and calculate total value, also, prepare having clause
grouped = merged_df.groupby('PS_PARTKEY').agg({'VALUE': ['sum']})
grouped.columns = ['TOTAL_VALUE']
threshold_percentage = 0.05
overall_value_for_germany = merged_df['VALUE'].sum()
threshold_value = overall_value_for_germany * threshold_percentage

# Filter groups having total value greater than the threshold
filtered_groups = grouped[grouped['TOTAL_VALUE'] > threshold_value]

# Output the final result to csv
filtered_groups = filtered_groups.sort_values(by='TOTAL_VALUE', ascending=False)
filtered_groups.to_csv('query_output.csv')
