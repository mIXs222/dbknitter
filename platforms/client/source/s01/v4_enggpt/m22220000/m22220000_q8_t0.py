import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime

# Connecting to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    cursorclass=pymysql.cursors.Cursor
)

try:
    mysql_cursor = mysql_conn.cursor()

    # Get the relevant data from MySQL tables
    mysql_cursor.execute("SELECT c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, l.L_EXTENDEDPRICE, l.L_DISCOUNT "
                         "FROM customer c "
                         "JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY "
                         "JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY "
                         "WHERE o.O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'")
    result_set_mysql = mysql_cursor.fetchall()
    df_mysql = pd.DataFrame(result_set_mysql, columns=['C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])
finally:
    mysql_cursor.close()
    mysql_conn.close()

# Connecting to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
region_data = pd.read_json(redis_conn.get('region'))
nation_data = pd.read_json(redis_conn.get('nation'))
part_data = pd.read_json(redis_conn.get('part'))
supplier_data = pd.read_json(redis_conn.get('supplier'))

# Filter data based on criteria from the Redis tables
asia_region_key = region_data[region_data['R_NAME'] == 'ASIA']['R_REGIONKEY'].values[0]
india_nation_keys = nation_data[(nation_data['N_REGIONKEY'] == asia_region_key) & (nation_data['N_NAME'] == 'INDIA')]['N_NATIONKEY'].values

# Filter suppliers from INDIAN suppliers
suppliers_from_india = supplier_data[supplier_data['S_NATIONKEY'].isin(india_nation_keys)]

# Filter parts of type 'SMALL PLATED COPPER'
small_plated_copper_parts = part_data[part_data['P_TYPE'] == 'SMALL PLATED COPPER']['P_PARTKEY'].values

# Merge dataframes to get the necessary joined data from Redis
df_redis_supplier_parts = pd.merge(suppliers_from_india, small_plated_copper_parts, how='inner', left_on='S_SUPPKEY', right_on='P_PARTKEY')

# Converting Orderdate to datetime and extracting year
df_mysql['O_ORDERDATE'] = pd.to_datetime(df_mysql['O_ORDERDATE'])
df_mysql['YEAR'] = df_mysql['O_ORDERDATE'].dt.year

# Calculate adjusted_price and join with nation and part data to compute volumes
df_mysql['ADJUSTED_PRICE'] = df_mysql['L_EXTENDEDPRICE'] * (1 - df_mysql['L_DISCOUNT'])
df_total_volume = df_mysql.groupby('YEAR')['ADJUSTED_PRICE'].sum().reset_index()
df_mysql_india = df_mysql[df_mysql['C_CUSTKEY'].isin(india_nation_keys)]

# Compute market share
df_market_share = df_mysql_india.groupby('YEAR')['ADJUSTED_PRICE'].sum().reset_index()
df_market_share = df_market_share.merge(df_total_volume, on='YEAR', suffixes=('_INDIA', '_TOTAL'))
df_market_share['MARKET_SHARE'] = df_market_share['ADJUSTED_PRICE_INDIA'] / df_market_share['ADJUSTED_PRICE_TOTAL']

# Write the output to file
df_market_share.sort_values(by='YEAR').to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
