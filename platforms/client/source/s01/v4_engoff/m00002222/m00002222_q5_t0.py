import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL Database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Get required tables from MySQL
with mysql_connection.cursor() as cursor:
    cursor.execute("SELECT * FROM nation;")
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

    cursor.execute("SELECT * FROM region WHERE R_NAME = 'ASIA';")
    asia_regions = pd.DataFrame(cursor.fetchall(), columns=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])

    cursor.execute("SELECT * FROM supplier;")
    suppliers = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Close MySQL connection
mysql_connection.close()

# Filter nations for those within ASIA region
asia_nations = nations[nations['N_REGIONKEY'].isin(asia_regions['R_REGIONKEY'])]

# Connect to Redis Database
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Load tables from Redis
customers = pd.read_json(redis_connection.get('customer'))
orders = pd.read_json(redis_connection.get('orders'))
lineitems = pd.read_json(redis_connection.get('lineitem'))

# Filter the date range for orders
orders = orders[(orders['O_ORDERDATE'] >= '1990-01-01') & (orders['O_ORDERDATE'] < '1995-01-01')]

# Join operations to compute the results
merged_data = (
    lineitems
    .merge(orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customers, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(suppliers, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(asia_nations, how='inner', left_on=['C_NATIONKEY', 'S_NATIONKEY'], right_on=['N_NATIONKEY', 'N_NATIONKEY'])
)

# Calculate revenue
merged_data['REVENUE'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])

# Sum up revenue by nation and sort
result = (
    merged_data
    .groupby(['N_NAME'])
    .agg({'REVENUE': 'sum'})
    .reset_index()
    .sort_values(by='REVENUE', ascending=False)
)

# Write to CSV
result.to_csv('query_output.csv', index=False)
