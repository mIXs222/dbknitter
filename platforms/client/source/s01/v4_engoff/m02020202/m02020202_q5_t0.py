import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL to get nations in ASIA region
mysql_query_nations_in_asia = """
SELECT n.N_NATIONKEY, n.N_NAME
FROM nation n
JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
WHERE r.R_NAME = 'ASIA'
"""
nations_in_asia_df = pd.read_sql(mysql_query_nations_in_asia, mysql_conn)

# Get tables from Redis
region_df = pd.DataFrame(redis_conn.get('region'))
supplier_df = pd.DataFrame(redis_conn.get('supplier'))
customer_df = pd.DataFrame(redis_conn.get('customer'))
lineitem_df = pd.DataFrame(redis_conn.get('lineitem'))

# Filter lineitems between specific dates
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= pd.Timestamp('1990-01-01')) &
    (lineitem_df['L_SHIPDATE'] <= pd.Timestamp('1995-01-01'))
]

# Combine the DataFrames to calculate revenue
combined_df = filtered_lineitem_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
combined_df = combined_df.merge(customer_df, left_on='L_ORDERKEY', right_on='C_CUSTKEY')
combined_df = combined_df[combined_df['S_NATIONKEY'] == combined_df['C_NATIONKEY']]
combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])

# Group by nation and calculate revenue
result_df = combined_df.groupby('S_NATIONKEY')['REVENUE'].sum().reset_index()
result_df = result_df.merge(nations_in_asia_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Select and sort by the revenue
result_df = result_df[['N_NAME', 'REVENUE']].sort_values(by='REVENUE', ascending=False)

# Output to CSV
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
redis_conn.close()
