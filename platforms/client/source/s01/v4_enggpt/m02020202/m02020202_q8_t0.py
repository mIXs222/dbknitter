import pymysql
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Query for 'nation' and 'part' tables
try:
    mysql_cursor.execute("SELECT * FROM nation")
    nation_data = mysql_cursor.fetchall()
    df_nation = pd.DataFrame(nation_data, columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

    mysql_cursor.execute("SELECT * FROM part WHERE P_TYPE='SMALL PLATED COPPER'")
    part_data = mysql_cursor.fetchall()
    df_part = pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])
finally:
    mysql_cursor.close()
    mysql_conn.close()

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)
df_region = pd.read_json(redis_conn.get('region'))
df_supplier = pd.read_json(redis_conn.get('supplier'))
df_customer = pd.read_json(redis_conn.get('customer'))
df_lineitem = pd.read_json(redis_conn.get('lineitem'))
df_orders = pd.read_json(redis_conn.get('orders'))

# Data merging and filtering
df = (
    df_orders[df_orders['O_ORDERDATE'].between(datetime(1995, 1, 1), datetime(1996, 12, 31))]
    .merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(df_part, left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(df_supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    .merge(df_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Focus on 'ASIA' region and nation 'INDIA'
asia_india_df = df[
    (df['R_NAME'] == 'ASIA') &
    (df['N_NAME'] == 'INDIA')
]

# Calculate total volume and INDIAN volume
asia_india_df['VOLUME'] = asia_india_df['L_EXTENDEDPRICE'] * (1 - asia_india_df['L_DISCOUNT'])
total_volume_by_year = df.groupby(df['O_ORDERDATE'].dt.year)['VOLUME'].sum()
indian_volume_by_year = asia_india_df.groupby(asia_india_df['O_ORDERDATE'].dt.year)['VOLUME'].sum()

# Calculate market share
market_share = (indian_volume_by_year / total_volume_by_year).reset_index()
market_share.columns = ['YEAR', 'MARKET_SHARE']

# Export to CSV
market_share.to_csv('query_output.csv', index=False)
