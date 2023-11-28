import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = mysql_conn.cursor()

# Query MySQL for part, supplier, nation, and region data
part_query = "SELECT * FROM part WHERE P_TYPE = 'SMALL PLATED COPPER';"
cursor.execute(part_query)
part_data = cursor.fetchall()
df_part = pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

supplier_query = "SELECT * FROM supplier;"
cursor.execute(supplier_query)
supplier_data = cursor.fetchall()
df_supplier = pd.DataFrame(supplier_data, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

nation_query = "SELECT * FROM nation;"
cursor.execute(nation_query)
nation_data = cursor.fetchall()
df_nation = pd.DataFrame(nation_data, columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

region_query = "SELECT * FROM region WHERE R_NAME = 'ASIA';"
cursor.execute(region_query)
region_data = cursor.fetchall()
df_region = pd.DataFrame(region_data, columns=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])

# Close MySQL connection
cursor.close()
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query Redis for customer, orders, lineitem data
df_customer = pd.read_json(redis_conn.get('customer'))
df_orders = pd.read_json(redis_conn.get('orders'))
df_lineitem = pd.read_json(redis_conn.get('lineitem'))

# Filter the orders for the specified time range
df_orders = df_orders[df_orders['O_ORDERDATE'].between('1995-01-01', '1996-12-31')]

# Merge data sets to perform the analysis
df_merged = pd.merge(df_lineitem, df_part, left_on='L_PARTKEY', right_on='P_PARTKEY')
df_merged = pd.merge(df_merged, df_supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df_merged = pd.merge(df_merged, df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df_merged = pd.merge(df_merged, df_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
df_merged = pd.merge(df_merged, df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df_merged = pd.merge(df_merged, df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Filter for the 'INDIA' nation within the 'ASIA' region
df_india = df_merged[(df_merged['N_NAME'] == 'INDIA') & (df_merged['R_NAME'] == 'ASIA')]

# Calculate the market share
df_india['Year'] = pd.DatetimeIndex(df_india['O_ORDERDATE']).year
df_india['Volume'] = df_india['L_EXTENDEDPRICE'] * (1 - df_india['L_DISCOUNT'])
grouped_india = df_india.groupby('Year')['Volume'].sum().reset_index(name='India_Volume')

# Calculate the total volume for comparison
df_merged['Volume'] = df_merged['L_EXTENDEDPRICE'] * (1 - df_merged['L_DISCOUNT'])
grouped_total = df_merged.groupby('Year')['Volume'].sum().reset_index(name='Total_Volume')

# Merge India and Total volumes and calculate market share
market_share = pd.merge(grouped_india, grouped_total, on='Year')
market_share['Market_Share'] = market_share['India_Volume'] / market_share['Total_Volume']

# Order the results by year and save to CSV
market_share = market_share.sort_values('Year')
market_share.to_csv('query_output.csv', index=False)
