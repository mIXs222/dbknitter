import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Read nation, region, and supplier tables from MySQL
nation_df = pd.read_sql("SELECT * FROM nation", mysql_conn)
region_df = pd.read_sql("SELECT * FROM region", mysql_conn)
supplier_df = pd.read_sql("SELECT * FROM supplier", mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(
    host='redis',
    port=6379,
    db=0
)

# Read customer, orders, and lineitem tables from Redis as Pandas DataFrames
customer_df = pd.read_msgpack(redis_conn.get('customer'))
orders_df = pd.read_msgpack(redis_conn.get('orders'))
lineitem_df = pd.read_msgpack(redis_conn.get('lineitem'))

# Filter based on conditions for orders and region
orders_filtered_df = orders_df[
    (orders_df['O_ORDERDATE'] >= '1990-01-01') & 
    (orders_df['O_ORDERDATE'] < '1995-01-01')
]
region_filtered_df = region_df[region_df['R_NAME'] == 'ASIA']

# Merge DataFrames to perform the SQL-like join and aggregation
query_result = (
    customer_df.merge(orders_filtered_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(supplier_df, left_on=['C_NATIONKEY', 'L_SUPPKEY'], right_on=['S_NATIONKEY', 'S_SUPPKEY'])
    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(region_filtered_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
    .groupby('N_NAME')
    .agg(REVENUE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: (x * (1 - lineitem_df.loc[x.index, 'L_DISCOUNT'])).sum()))
    .reset_index()
    .sort_values(by='REVENUE', ascending=False)
)

# Write the output to a CSV file
query_result.to_csv('query_output.csv', index=False)
