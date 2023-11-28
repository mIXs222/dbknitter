# import necessary libraries
import pymysql
import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to the mysql database
mysql_con = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Execute queries to fetch relevant data from MySQL tables
with mysql_con.cursor() as cursor:
    # Fetch data from nation and region tables
    cursor.execute("SELECT * FROM nation INNER JOIN region ON nation.N_REGIONKEY = region.R_REGIONKEY WHERE region.R_NAME = 'ASIA'")
    nation_region_df = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT', 'R_REGIONKEY', 'R_NAME', 'R_COMMENT'])

    # Fetch data from supplier table
    cursor.execute("SELECT * FROM supplier")
    supplier_df = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Connect to the redis database by creating a DirectRedis client instance
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load data from Redis into pandas DataFrames using get method
customer_df = pd.read_json(redis_client.get('customer'))
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Perform data filtering and merging operations
# Filter orders within the given date range
start_date = datetime.datetime(1990, 1, 1)
end_date = datetime.datetime(1994, 12, 31)
filtered_orders_df = orders_df[(orders_df['O_ORDERDATE'] >= start_date) & (orders_df['O_ORDERDATE'] <= end_date)]

# Merge dataframes based on keys and perform calculations
merged_data = (
    customer_df
    .merge(filtered_orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation_region_df, left_on=['C_NATIONKEY', 'S_NATIONKEY'], right_on=['N_NATIONKEY', 'N_NATIONKEY'])
)

# Calculate the total revenue
merged_data['REVENUE'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])

# Group by nation name and calculate total revenue
result = (
    merged_data.groupby('N_NAME')
    .agg({'REVENUE': 'sum'})
    .reset_index()
    .sort_values(by='REVENUE', ascending=False)
)

# Save the result to query_output.csv
result.to_csv('query_output.csv', index=False)

# Close the mysql connection
mysql_con.close()
