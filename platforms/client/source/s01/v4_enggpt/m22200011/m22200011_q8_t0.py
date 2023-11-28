# file: query.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query and fetching data from MySQL
with mysql_conn.cursor() as cursor:
    # Fetching data from supplier table
    cursor.execute("SELECT * FROM supplier;")
    suppliers_data = cursor.fetchall()
    # Fetching customer info for the specified market segment
    cursor.execute("SELECT * FROM customer WHERE C_MKTSEGMENT='SMALL PLATED COPPER';")
    customers_data = cursor.fetchall()

# Convert the MySQL data into Pandas DataFrames
suppliers_df = pd.DataFrame(suppliers_data, columns=['S_SUPPKEY', 'S_NAME',
                                                     'S_ADDRESS', 'S_NATIONKEY',
                                                     'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

customers_df = pd.DataFrame(customers_data, columns=['C_CUSTKEY', 'C_NAME',
                                                     'C_ADDRESS', 'C_NATIONKEY',
                                                     'C_PHONE', 'C_ACCTBAL',
                                                     'C_MKTSEGMENT', 'C_COMMENT'])

# Querying data from MongoDB
orders_coll = mongodb_db['orders']
lineitem_coll = mongodb_db['lineitem']

# Filtering orders between 1995 and 1996 and projecting necessary fields
orders_query = {'O_ORDERDATE': {'$gte': '1995-01-01', '$lte': '1996-12-31'}}
orders_fields = {'_id': 0, 'O_ORDERKEY': 1, 'O_CUSTKEY': 1, 'O_ORDERDATE': 1}
orders_df = pd.DataFrame(list(orders_coll.find(orders_query, orders_fields)))

# Projecting necessary fields for the lineitem table
lineitem_fields = {'_id': 0, 'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1, 'L_SUPPKEY': 1}
lineitem_df = pd.DataFrame(list(lineitem_coll.find({}, lineitem_fields)))

# Fetching data from Redis using DataFrame
nation_df = pd.read_json(redis_client.get('nation').decode('utf-8'))
region_df = pd.read_json(redis_client.get('region').decode('utf-8'))

# Filter only ASIA region and INDIA nation
asia_region = region_df.query("R_NAME == 'ASIA'")
india_nation = nation_df.query("N_NAME == 'INDIA' and N_REGIONKEY in @asia_region.R_REGIONKEY")

# Merge dataframes on appropriate keys to get the relevant rows
df_merged = pd.merge(lineitem_df, suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df_merged = pd.merge(df_merged, customers_df, left_on='S_SUPPKEY', right_on='C_NATIONKEY')
df_merged = pd.merge(df_merged, india_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
df_merged = pd.merge(df_merged, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Calculate the volume and compute market share for INDIA
df_merged['VOLUME'] = df_merged['L_EXTENDEDPRICE'] * (1 - df_merged['L_DISCOUNT'])

# Group by year and sum VOLUME
df_merged['YEAR'] = df_merged['O_ORDERDATE'].str[:4]
volume_by_year = df_merged.groupby('YEAR')['VOLUME'].sum().reset_index()

# Calculate the total volume for reference
total_volume = df_merged['VOLUME'].sum()

# Calculate the market share
volume_by_year['MARKET_SHARE'] = volume_by_year['VOLUME'] / total_volume

# Sort results by year
volume_by_year.sort_values('YEAR', ascending=True, inplace=True)

# Write the result to CSV
volume_by_year.to_csv('query_output.csv', index=False)

# Close all connections
mysql_conn.close()
mongodb_client.close()
redis_client.connection_pool.disconnect()
