import pymysql
import pandas as pd
from sqlalchemy import create_engine
import direct_redis

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Redis Connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch relevant tables from MySQL
lineitem_query = """
SELECT
    L_SUPPKEY,
    L_PARTKEY,
    L_QUANTITY
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01';
"""
supplier_query = "SELECT S_SUPPKEY, S_NAME FROM supplier;"
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)
supplier_df = pd.read_sql(supplier_query, mysql_conn)

# Fetch relevant tables from Redis
nation_df = pd.read_json(r.get('nation'), orient='records')
part_df = pd.read_json(r.get('part'), orient='records')
partsupp_df = pd.read_json(r.get('partsupp'), orient='records')

# Close MySQL connection
mysql_conn.close()

# Filter nations for Canada
nation_df = nation_df[nation_df['N_NAME'] == 'CANADA']

# Merge DataFrame
combined_df = (
    part_df.merge(partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
    .merge(lineitem_df, how='inner', left_on=['P_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
    .merge(nation_df, how='inner', left_on='PS_SUPPKEY', right_on='N_NATIONKEY')
    .merge(supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
)

# Consider only forest parts based on naming convention
forest_parts_df = combined_df[combined_df['P_NAME'].str.contains('forest', case=False)]

# Compute excess of part quantity shipped
forest_parts_df['TOTAL_QUANTITY'] = forest_parts_df.groupby(['S_SUPPKEY'])['L_QUANTITY'].transform('sum')
forest_parts_df = forest_parts_df[forest_parts_df['TOTAL_QUANTITY'] > (forest_parts_df['L_QUANTITY'] * 1.5)]

# Select the required columns for output
output_df = forest_parts_df[['S_SUPPKEY', 'S_NAME']].drop_duplicates()

# Write the output to a csv file
output_df.to_csv('query_output.csv', index=False)
