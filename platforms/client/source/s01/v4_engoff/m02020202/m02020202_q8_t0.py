# query.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Retrieving MySQL tables
with mysql_conn.cursor() as mysql_cursor:
    mysql_cursor.execute("SELECT * FROM nation;")
    nations = pd.DataFrame(mysql_cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

    mysql_cursor.execute("SELECT * FROM part WHERE P_TYPE = 'SMALL PLATED COPPER';")
    parts = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

    mysql_cursor.execute("SELECT * FROM orders WHERE YEAR(O_ORDERDATE) IN (1995, 1996);")
    orders = pd.DataFrame(mysql_cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

mysql_conn.close()

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieving Redis tables
region = pd.read_msgpack(redis_conn.get('region'))
supplier = pd.read_msgpack(redis_conn.get('supplier'))
customer = pd.read_msgpack(redis_conn.get('customer'))
lineitem = pd.read_msgpack(redis_conn.get('lineitem'))

# Data transformation and query execution
asia_regions = region[region['R_NAME'] == 'ASIA']['R_REGIONKEY']
suppliers_india = supplier[supplier['S_NATIONKEY'].isin(
    nations[nations['N_NAME'] == 'INDIA']['N_NATIONKEY'])]['S_SUPPKEY']
lineitem_filtered = lineitem[lineitem['L_PARTKEY'].isin(parts['P_PARTKEY'])]

# Aggregating the required data
orders_filtered = orders[
    orders['O_ORDERKEY'].isin(lineitem_filtered['L_ORDERKEY'])
    & orders['O_ORDERSTATUS'] == 'F']  # Assuming 'F' stands for Finished, as there is no O_ORDERSTATUS criteria given in the task
orders_merged = orders_filtered.merge(customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
orders_in_asia_india = orders_merged[
    orders_merged['C_NATIONKEY'].isin(nations[nations['N_REGIONKEY'].isin(asia_regions)]['N_NATIONKEY'])
    & orders_merged['O_TOTALPRICE'].notnull()]

revenue_by_year = (
    lineitem_filtered[lineitem_filtered['L_SUPPKEY'].isin(suppliers_india)]
    .assign(revenue=lambda df: df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT']))
    .groupby(orders_filtered['O_ORDERDATE'].dt.year)['revenue'].sum()
)

# Result: Market Share
market_share = revenue_by_year / orders_in_asia_india.groupby(orders_in_asia_india['O_ORDERDATE'].dt.year)['O_TOTALPRICE'].sum()

# Saving the result to CSV
market_share.to_csv('query_output.csv', header=['Market Share'], index_label='Year')
