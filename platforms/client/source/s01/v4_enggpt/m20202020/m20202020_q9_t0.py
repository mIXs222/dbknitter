import pymysql
import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query MySQL database for lineitem and supplier tables
mysql_query = """
SELECT 
    s.S_NATIONKEY, 
    l.L_ORDERKEY,
    l.L_PARTKEY,
    l.L_SUPPKEY,
    l.L_EXTENDEDPRICE,
    l.L_DISCOUNT,
    l.L_QUANTITY
FROM 
    lineitem l
JOIN 
    supplier s ON l.L_SUPPKEY = s.S_SUPPKEY;
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    # Fetch the result
    lineitem_supplier_data = cursor.fetchall()

# Convert fetched data to pandas DataFrame
lineitem_supplier_df = pd.DataFrame(lineitem_supplier_data, columns=[
    'S_NATIONKEY',
    'L_ORDERKEY',
    'L_PARTKEY',
    'L_SUPPKEY',
    'L_EXTENDEDPRICE',
    'L_DISCOUNT',
    'L_QUANTITY'
])

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis and convert to pandas DataFrames
nation_df = pd.read_msgpack(redis.get('nation'))
part_df = pd.read_msgpack(redis.get('part'))
partsupp_df = pd.read_msgpack(redis.get('partsupp'))
orders_df = pd.read_msgpack(redis.get('orders'))

# Only consider parts with names containing "dim"
part_df = part_df[part_df['P_NAME'].str.contains('dim')]

# Merge DataFrames to perform calculations
merged_df = lineitem_supplier_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(partsupp_df, on=['L_PARTKEY', 'L_SUPPKEY'])
merged_df = merged_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate profit
merged_df['PROFIT'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])

# Extract year from the order date
merged_df['YEAR'] = merged_df['O_ORDERDATE'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').year)

# Group by nation and year
profit_distribution = merged_df.groupby(['N_NAME', 'YEAR'])['PROFIT'].sum().reset_index()

# Sort the results
profit_distribution_sorted = profit_distribution.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False])

# Write to CSV
profit_distribution_sorted.to_csv('query_output.csv', index=False)

# Close the MySQL connection
mysql_conn.close()
