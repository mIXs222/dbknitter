import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query for MySQL tables
mysql_query = """
SELECT
    p.P_PARTKEY,
    s.S_SUPPKEY,
    s.S_NATIONKEY AS SUPPLIER_NATION,
    n1.N_NATIONKEY AS CUSTOMER_NATION,
    n1.N_REGIONKEY,
    r.R_NAME,
    n2.N_NAME
FROM
    part p,
    supplier s,
    nation n1,
    nation n2,
    region r
WHERE
    s.S_NATIONKEY = n2.N_NATIONKEY
    AND n1.N_REGIONKEY = r.R_REGIONKEY
    AND r.R_NAME = 'ASIA'
    AND p.P_TYPE = 'SMALL PLATED COPPER'
"""

# Load MySQL tables
mysql_df = pd.read_sql(mysql_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load Redis tables
customer_df = pd.read_json(redis_conn.get('customer'))
orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Merge Redis tables
redis_merged_df = lineitem_df.merge(
    orders_df,
    left_on='L_ORDERKEY',
    right_on='O_ORDERKEY'
).merge(
    customer_df,
    left_on='O_CUSTKEY',
    right_on='C_CUSTKEY'
)

# Filter Redis data on order date
redis_merged_df = redis_merged_df[
    (redis_merged_df['O_ORDERDATE'] >= '1995-01-01') &
    (redis_merged_df['O_ORDERDATE'] <= '1996-12-31')
]
redis_merged_df['O_YEAR'] = pd.to_datetime(redis_merged_df['O_ORDERDATE']).dt.year

# Merge MySQL & Redis data
result = redis_merged_df.merge(
    mysql_df,
    how='inner',
    left_on=['L_PARTKEY', 'L_SUPPKEY', 'C_NATIONKEY'],
    right_on=['P_PARTKEY', 'S_SUPPKEY', 'CUSTOMER_NATION']
)
result = result[result['R_NAME'] == 'ASIA']  # Additional filter for 'ASIA'

# Calculate volume and apply conditions for Indian market share
result['VOLUME'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])
result['NATION'] = result['N_NAME']
result['INDIA_VOLUME'] = result.apply(
    lambda x: x['VOLUME'] if x['NATION'] == 'INDIA' else 0,
    axis=1
)

# Group by year and calculate market share
final_result = result.groupby('O_YEAR').agg({
    'VOLUME': 'sum',
    'INDIA_VOLUME': 'sum'
}).reset_index()
final_result['MKT_SHARE'] = final_result['INDIA_VOLUME'] / final_result['VOLUME']

# Select required fields
final_result = final_result[['O_YEAR', 'MKT_SHARE']]

# Write to CSV
final_result.to_csv('query_output.csv', index=False)
