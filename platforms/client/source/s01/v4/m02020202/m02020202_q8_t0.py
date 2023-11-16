import pymysql
import pandas as pd
import direct_redis

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Load data from MySQL tables into pandas DataFrames
part_df = pd.read_sql("SELECT * FROM part WHERE P_TYPE = 'SMALL PLATED COPPER'", mysql_conn)
orders_df = pd.read_sql("SELECT * FROM orders WHERE O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'", mysql_conn)
nation_df = pd.read_sql("SELECT * FROM nation", mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis and load data into pandas DataFrames
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

region_df = pd.read_msgpack(redis_conn.get('region'))
supplier_df = pd.read_msgpack(redis_conn.get('supplier'))
customer_df = pd.read_msgpack(redis_conn.get('customer'))
lineitem_df = pd.read_msgpack(redis_conn.get('lineitem'))

# Merge the DataFrames to mimic the SQL join operations
merged_df = (
    part_df.merge(lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')
           .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
           .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
           .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
           .merge(nation_df.rename(columns={'N_NATIONKEY': 'C_NATIONKEY', 'N_NAME': 'C_NAME'}), on='C_NATIONKEY')
           .merge(nation_df.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'S_NAME'}), on='S_NATIONKEY')
           .merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Filter the merged DataFrame for ASIA region
asia_df = merged_df[merged_df['R_NAME'] == 'ASIA']

# Calculate the volume and filter out the records by the nation (INDIA)
asia_df['VOLUME'] = asia_df['L_EXTENDEDPRICE'] * (1 - asia_df['L_DISCOUNT'])
asia_df['O_YEAR'] = asia_df['O_ORDERDATE'].dt.year
india_df = asia_df[asia_df['S_NAME'] == 'INDIA']

# Group by O_YEAR and calculate market share
grouped = asia_df.groupby('O_YEAR')
india_grouped = india_df.groupby('O_YEAR')
market_share_df = (india_grouped['VOLUME'].sum() / grouped['VOLUME'].sum()).reset_index()
market_share_df.columns = ['O_YEAR', 'MKT_SHARE']

# Write the query's output to the file
market_share_df.to_csv('query_output.csv', index=False)
