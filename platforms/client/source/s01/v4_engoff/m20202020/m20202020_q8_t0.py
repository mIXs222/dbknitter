import pymysql
import direct_redis
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Query for MySQL
mysql_query = """
SELECT 
    S_SUPPKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_SHIPDATE
FROM 
    lineitem
INNER JOIN 
    supplier ON L_SUPPKEY = S_SUPPKEY
INNER JOIN 
    region ON S_NATIONKEY = region.R_REGIONKEY
WHERE 
    region.R_NAME = 'ASIA'
"""

# Execute the query in mysql and get the dataframe
lineitem_supplier_region_df = pd.read_sql(mysql_query, mysql_conn)

# Cleaning MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the dataframes from Redis
nation_df = redis_conn.get('nation')
part_df = redis_conn.get('part')

# Filter for 'INDIA' suppliers and SMALL PLATED COPPER parts
india_nations = nation_df[nation_df['N_NAME'] == 'INDIA']
spc_parts = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']

# Merge dataframes to filter relevant lineitems
merged_df = pd.merge(lineitem_supplier_region_df, india_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = pd.merge(merged_df, spc_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filter by years 1995 and 1996, and calculate fractions
merged_df['YEAR'] = pd.to_datetime(merged_df['L_SHIPDATE']).dt.year
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
merged_df = merged_df[(merged_df['YEAR'] == 1995) | (merged_df['YEAR'] == 1996)]
annual_revenue = merged_df.groupby('YEAR')['REVENUE'].sum()

# Calculate market share for INDA within ASIA
total_revenue = lineitem_supplier_region_df[lineitem_supplier_region_df['L_SHIPDATE'].str.contains("1995|1996")]
total_revenue['REVENUE'] = total_revenue['L_EXTENDEDPRICE'] * (1 - total_revenue['L_DISCOUNT'])
total_revenue['YEAR'] = pd.to_datetime(total_revenue['L_SHIPDATE']).dt.year
total_annual_revenue = total_revenue.groupby('YEAR')['REVENUE'].sum()

market_share = annual_revenue / total_annual_revenue

# Output to CSV file
market_share.to_csv('query_output.csv', header=['MARKET_SHARE'])
