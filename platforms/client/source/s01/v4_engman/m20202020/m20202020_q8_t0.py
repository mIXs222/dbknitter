import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Fetching Asia's and India's region key from region
query_asia_region = "SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA';"
asia_region_key = pd.read_sql(query_asia_region, mysql_connection).iloc[0, 0]

query_india_nation = f"SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'INDIA' AND N_REGIONKEY = {asia_region_key};"
india_nation_key = redis_connection.get('nation')
df_india_nation = pd.read_json(india_nation_key)
india_nation_key = df_india_nation.loc[df_india_nation['N_NAME'] == 'INDIA']['N_NATIONKEY'].iloc[0]

# Get suppliers from India
query_suppliers_india = f"SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY = {india_nation_key};"
suppliers_india = pd.read_sql(query_suppliers_india, mysql_connection)

# Get relevant lineitem data
lineitem_columns = ['L_SUPPKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPDATE']
query_lineitem = "SELECT L_SUPPKEY, L_EXTENDEDPRICE, L_DISCOUNT, YEAR(L_SHIPDATE) AS L_SHIPYEAR " \
                 "FROM lineitem WHERE L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31';"
df_lineitem = pd.read_sql(query_lineitem, mysql_connection)
df_lineitem = df_lineitem[df_lineitem['L_SUPPKEY'].isin(suppliers_india['S_SUPPKEY'])]

# Get parts of type 'SMALL PLATED COPPER'
parts_data = redis_connection.get('part')
df_parts = pd.read_json(parts_data)
df_parts = df_parts[df_parts['P_TYPE'] == 'SMALL PLATED COPPER']

# Combine the data
df_lineitem['REVENUE'] = df_lineitem['L_EXTENDEDPRICE'] * (1 - df_lineitem['L_DISCOUNT'])
df_combined = df_lineitem[df_lineitem['L_SUPPKEY'].isin(suppliers_india['S_SUPPKEY'])]
df_combined = df_combined.merge(df_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate market share
market_share = df_combined.groupby('L_SHIPYEAR')['REVENUE'].sum().reset_index()
market_share.columns = ['ORDER_YEAR', 'MARKET_SHARE']

# Write result to CSV
market_share.to_csv('query_output.csv', index=False)

# Close connections
mysql_connection.close()
