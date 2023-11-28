import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute query on MySQL tpch.partsupp table
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST FROM partsupp")
    partsupp_data = cursor.fetchall()

mysql_conn.close()

# Convert to pandas DataFrame
partsupp_df = pd.DataFrame(partsupp_data, columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST'])

# Function to extract data from Redis and create a pandas DataFrame
def fetch_redis_table(table_name):
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    table_data = redis_client.get(table_name)
    return pd.read_json(table_data)

# Fetch data from Redis
nation_df = fetch_redis_table('nation')
supplier_df = fetch_redis_table('supplier')

# Filter suppliers located in Germany
german_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df[nation_df['N_NAME'] == 'GERMANY']['N_NATIONKEY'])]

# Merge tables
merged_df = part_sup_df.merge(german_suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculate value
merged_df['VALUE'] = merged_df['PS_AVAILQTY'] * merged_df['PS_SUPPLYCOST']
total_value = merged_df['VALUE'].sum()
threshold_percent = 0.05  # define the certain percentage, as per requirement

# Filter groups
grouped_df = merged_df.groupby('PS_PARTKEY').agg(TOTAL_VALUE=('VALUE', 'sum'))
filtered_grouped_df = grouped_df[grouped_df['TOTAL_VALUE'] > total_value * threshold_percent]

# Sort results
sorted_df = filtered_grouped_df.sort_values(by='TOTAL_VALUE', ascending=False)

# Write results to CSV file
sorted_df.to_csv('query_output.csv')
