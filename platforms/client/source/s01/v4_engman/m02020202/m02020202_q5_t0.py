import pandas as pd
import pymysql
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch nation and orders from MySQL
mysql_cursor.execute('SELECT N_NATIONKEY, N_NAME FROM nation')
nations = pd.DataFrame(mysql_cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME'])

mysql_cursor.execute("SELECT O_ORDERKEY, O_CUSTKEY FROM orders WHERE O_ORDERDATE >= '1990-01-01' AND O_ORDERDATE < '1995-01-01'")
orders = pd.DataFrame(mysql_cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY'])

# Close MySQL cursor and connection
mysql_cursor.close()
mysql_conn.close()

# Fetch region, supplier, and customer data from Redis
region = pd.read_json(redis_conn.get('region'))
supplier = pd.read_json(redis_conn.get('supplier'))
customer = pd.read_json(redis_conn.get('customer'))
lineitem = pd.read_json(redis_conn.get('lineitem'))

# Filter region to ASIA
asian_region_keys = region[region['R_NAME'] == 'ASIA']['R_REGIONKEY'].tolist()

# Filter suppliers and customers in ASIA
asian_suppliers = supplier[supplier['S_NATIONKEY'].isin(asian_region_keys)]
asian_customers = customer[customer['C_NATIONKEY'].isin(asian_region_keys)]

# Merge tables to filter lineitems
asian_lineitems = pd.merge(
    lineitem,
    orders,
    left_on='L_ORDERKEY',
    right_on='O_ORDERKEY'
).merge(
    asian_customers,
    left_on='O_CUSTKEY',
    right_on='C_CUSTKEY'
).merge(
    asian_suppliers,
    left_on='L_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Calculate revenue
asian_lineitems['REVENUE'] = asian_lineitems['L_EXTENDEDPRICE'] * (1 - asian_lineitems['L_DISCOUNT'])

# Sum revenue by nation
revenue_by_nation = asian_lineitems.groupby('S_NATIONKEY')['REVENUE'].sum().reset_index()

# Merge with nations to get nation names
revenue_by_nation = revenue_by_nation.merge(nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Select relevant columns and sort by revenue
revenue_by_nation = revenue_by_nation[['N_NAME', 'REVENUE']].sort_values('REVENUE', ascending=False)

# Write to CSV
revenue_by_nation.to_csv('query_output.csv', index=False)
