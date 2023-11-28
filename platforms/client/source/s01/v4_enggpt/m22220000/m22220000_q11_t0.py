import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to get data from Redis and convert it to a DataFrame
def redis_to_dataframe(redis_connection, key):
    data = redis_connection.get(key)
    return pd.read_json(data)

# Function to execute SQL and return a DataFrame
def mysql_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    return pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Querying MySQL for partsupp data
partsupp_query = """
SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST
FROM partsupp
"""
partsupp_df = mysql_query(mysql_conn, partsupp_query)

# Getting data from Redis
nation_df = redis_to_dataframe(redis_conn, 'nation')
supplier_df = redis_to_dataframe(redis_conn, 'supplier')

# Merge dataframes to create the required dataset
merged_df = partsupp_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter for Germany suppliers
germany_suppliers_df = merged_df[merged_df['N_NAME'] == 'GERMANY']

# Calculate total value and filter for the specified threshold
germany_suppliers_df['TOTAL_VALUE'] = (germany_suppliers_df['PS_SUPPLYCOST'] *
                                       germany_suppliers_df['PS_AVAILQTY'])
total_value_threshold = germany_suppliers_df['TOTAL_VALUE'].sum() * 0.05
filtered_df = germany_suppliers_df.groupby('PS_PARTKEY').filter(
    lambda p: p['TOTAL_VALUE'].sum() > total_value_threshold)

# Group by PS_PARTKEY and order by TOTAL_VALUE desc
result_df = filtered_df.groupby('PS_PARTKEY').sum().reset_index()
result_df = result_df[['PS_PARTKEY', 'TOTAL_VALUE']].sort_values(by='TOTAL_VALUE', ascending=False)

# Write to CSV file
result_df.to_csv('query_output.csv', index=False)

# Closing connections
mysql_conn.close()
redis_conn.close()
