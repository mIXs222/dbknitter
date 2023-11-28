# Import necessary libraries
import pymysql
import pymongo
import pandas as pd
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
# Redis connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load data from MySQL
with mysql_conn.cursor() as cursor:
    # Load 'part' table filtering for 'SMALL PLATED COPPER' and 'region' table filtering for 'ASIA'
    part_query = "SELECT * FROM part WHERE P_TYPE = 'SMALL PLATED COPPER'"
    cursor.execute(part_query)
    parts = cursor.fetchall()
    parts_df = pd.DataFrame(parts, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])
    
    region_query = "SELECT * FROM region WHERE R_NAME = 'ASIA'"
    cursor.execute(region_query)
    regions = cursor.fetchall()
    region_df = pd.DataFrame(regions, columns=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])

    nation_query = "SELECT * FROM nation"
    cursor.execute(nation_query)
    nations = cursor.fetchall()
    nation_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

mysql_conn.close()

# Load data from MongoDB
suppliers_cursor = mongodb_db.supplier.find()
suppliers_df = pd.DataFrame(list(suppliers_cursor))
customers_cursor = mongodb_db.customer.find()
customers_df = pd.DataFrame(list(customers_cursor))

# Load data from Redis
orders_data = redis_conn.get('orders')
orders_df = pd.read_json(orders_data)
lineitem_data = redis_conn.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Format date columns for filtering
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
# Filter data for years 1995 and 1996
orders_df = orders_df[(orders_df['O_ORDERDATE'].dt.year == 1995) | (orders_df['O_ORDERDATE'].dt.year == 1996)]

# Merge tables to obtain the relevant data
merged_df = (orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
                      .merge(parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
                      .merge(suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
                      .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
                      .merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
                      .merge(customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY'))

# Calculate volume and filter by 'INDIA' nation
merged_df['VOLUME'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
filtered_df = merged_df[(merged_df['R_NAME'] == 'ASIA') & (merged_df['N_NAME'] == 'INDIA')]

# Calculate market share
volume_by_year = filtered_df.groupby(orders_df['O_ORDERDATE'].dt.year)['VOLUME'].sum().reset_index()
total_volume_by_year = merged_df.groupby(orders_df['O_ORDERDATE'].dt.year)['VOLUME'].sum().reset_index()
market_share = volume_by_year.merge(total_volume_by_year, on='O_ORDERDATE', suffixes=('_INDIA', '_TOTAL'))
market_share['MARKET_SHARE'] = market_share['VOLUME_INDIA'] / market_share['VOLUME_TOTAL']

# Select and order the results
final_df = market_share[['O_ORDERDATE', 'MARKET_SHARE']].sort_values(by='O_ORDERDATE')

# Write to CSV
final_df.to_csv('query_output.csv', index=False)
